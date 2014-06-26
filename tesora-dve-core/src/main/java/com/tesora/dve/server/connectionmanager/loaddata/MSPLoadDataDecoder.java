package com.tesora.dve.server.connectionmanager.loaddata;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.db.mysql.portal.MSPCommandHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang.ArrayUtils;

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.MyLoadDataInfileContext;
import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryPlanner;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPLoadDataDecoder extends ByteToMessageDecoder {

	public static final String LOAD_DATA_DECODER_NAME = "LoadDataDecoder";

    private static final int EMPTYPKT_INDICATOR = 0x0;
    public static final String MSP_COMMAND_HANDLER = MSPCommandHandler.class.getSimpleName();

    private ExecutorService clientExecutorService = null;
	private SSConnection ssCon = null;
	MyLoadDataInfileContext myLoadDataInfileContext = null;
	private int length = 0;
	private ByteBuf frame = null;
	static private MyOKResponse okResp = new MyOKResponse();
	List<Future<Void>> dataBlockWorkerList = new ArrayList<Future<Void>>();
	Throwable t = null;
	
	public enum MyLoadDataDecoderState {
		READ_DATA_BLOCK, READ_EOF;
	}

	public MSPLoadDataDecoder(ExecutorService clientExecutorService, SSConnection ssCon, MyLoadDataInfileContext myLoadDataInfileContext) {
		this.clientExecutorService = clientExecutorService;
		this.ssCon = ssCon;
		this.myLoadDataInfileContext = myLoadDataInfileContext;
		dataBlockWorkerList.clear();
	}

	@Override
	protected void decode(final ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

		if (in.readableBytes() < 3) {
			return;
		}

		in.markReaderIndex();

		ByteBuf buffer = in.order(ByteOrder.LITTLE_ENDIAN);
		length = buffer.readUnsignedMedium();
		
		if (buffer.readableBytes() < length - 3) {
			in.resetReaderIndex();
			return;
		}

        //SMG: This bytebuf is a heap buffer, but we shouldn't assume so and manage the refcount like it was a direct buffer, in case it changes.
		frame = buffer.readBytes(length+1);			// +1 for the packet number

		boolean removeDecoder = true;
		try {
			
			MyLoadDataInfileContext loadDataInfileCtx = MyLoadDataInfileContext.getLoadDataInfileContextFromChannel(ctx);
			if (loadDataInfileCtx == null) {
				MyLoadDataInfileContext.setLoadDataInfileContextOnChannel(ctx, myLoadDataInfileContext);
			}

			if (length == EMPTYPKT_INDICATOR) {
                handleEOFPacket(ctx, loadDataInfileCtx);
            } else {
                removeDecoder = handleNormalFrame(ctx);
			}
		} finally {
			if (removeDecoder) {
				ctx.pipeline().remove(this);
                //TODO: double check if this release is required.  I would expect ByteToMessageDecoder to always release the input buffer, even if we get removed from pipeline. -sgossard
				in.release();
			}
		}
	}

    private boolean handleNormalFrame(ChannelHandlerContext ctx) {
        try {
            frame.readByte(); //packet number
            byte[] msgBuf = MysqlAPIUtils.readBytes(frame);
            if (t == null) {
                //SMG: NOTE: previously there was a get() that keep netty from generating too many execute() calls and risking out of order execution.  execute() batches must be serialized. -sgossard
                List<List<byte[]>> parsedRows = LoadDataBlockExecutor.processDataBlock(ctx, ssCon, msgBuf);
                dataBlockWorkerList.add(execute(ctx, parsedRows));
            }
        } catch (Throwable e) {
            // if we get an error we still need to drain the data coming from the client
            // so capture the fact we have an error
            t = e;
        }
        return false;
    }

    private void handleEOFPacket(ChannelHandlerContext ctx, MyLoadDataInfileContext loadDataInfileCtx) {
        try {
            if (t == null) {
                //TODO: We pass an empty byte array here to signal EOF, but I don't see anything downstream to deal with an incomplete row on eof. -sgossard
                List<List<byte[]>> parsedRows = LoadDataBlockExecutor.processDataBlock(ctx, ssCon, ArrayUtils.EMPTY_BYTE_ARRAY);
                dataBlockWorkerList.add(execute(ctx, parsedRows));
                for (Future<Void> future : dataBlockWorkerList) {
                    //SMG: this future get blocks the frontend netty thread to wait for all the executes to succeed before we send out an OK, or send an error. -sgossard
                    future.get();
                }
                sendResponseOnMainCtx(ctx, createLoadDataEOFMsg(loadDataInfileCtx, frame.readByte()));
            } else {
                sendResponseOnMainCtx(ctx, new MyErrorResponse(new PEException(t)));
            }
        } catch (Throwable e) {
            sendResponseOnMainCtx(ctx, new MyErrorResponse(new PEException(e)));
        } finally {
            closePreparedStatements(ctx, loadDataInfileCtx);
        }
    }

    private void closePreparedStatements(ChannelHandlerContext ctx, MyLoadDataInfileContext loadDataInfileCtx) {
		for (MyPreparedStatement<String> pStmt : loadDataInfileCtx.getPreparedStatements()) {
			ssCon.removePreparedStatement(pStmt.getStmtId());
			QueryPlanner.destroyPreparedStatement(ssCon, pStmt.getStmtId());
		}
		loadDataInfileCtx.clearPreparedStatements();
	}

	static public MyOKResponse createLoadDataEOFMsg(MyLoadDataInfileContext loadDataInfileCtx, byte packetNumber) throws PEException {
		MyOKResponse okResp = getOkResp();
		okResp.setAffectedRows(loadDataInfileCtx.getInfileRowsAffected());
		okResp.setWarningCount((short) loadDataInfileCtx.getInfileWarnings());
		okResp.setInsertId(0);
		okResp.setStatusInTrans(false);
		okResp.withPacketNumber(packetNumber);

		return okResp;
	}

	static private MyOKResponse getOkResp() {
		if (okResp == null) {
			okResp = new MyOKResponse();
		}
		return okResp;
	}

	private void sendResponseOnMainCtx(ChannelHandlerContext ctx, Object msg) {
		ctx.pipeline().context(MSP_COMMAND_HANDLER).writeAndFlush(msg);
	}

    private Future<Void> execute(final ChannelHandlerContext ctx, final List<List<byte[]>> dataRows) {
		return clientExecutorService.submit(new Callable<Void>() {
			public Void call() throws Exception {
				ssCon.executeInContext(new Callable<Void>() {
					public Void call()  {
						try {
							LoadDataBlockExecutor.executeInsert(ctx, ssCon, dataRows);
						} catch (Throwable e) {
							sendResponseOnMainCtx(ctx, new MyErrorResponse(new PEException(e)));
						}
						return null;
					}
				});
				return null;
			}
		});
	}
}
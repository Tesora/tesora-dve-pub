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
import com.tesora.dve.db.mysql.libmy.MyMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import io.netty.util.ReferenceCountUtil;

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.MyLoadDataInfileContext;
import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryPlanner;
import com.tesora.dve.server.connectionmanager.SSConnection;

/**
 * A netty handler that decodes and dispatches "LOAD DATA LOCAL INFILE", where the file contents are provided on the socket rather than on the server's disk.
 * Like all netty handlers, methods called by netty should never block since this will prevent other sockets processed by the same
 * thread from being serviced.  Currently, this class only expects method invocations from a single netty thread,
 * and is therefore completely thread safe and doesn't require any locking or atomics.  The only exception to this rule is
 * inserts are blocking and get submitted to a separate thread pool to prevent blocking, and the request and responses are handled
 * on client executor threads, not the normal netty thread.  To make this boundary clear, any code executed off the netty
 * thread has been moved to methods called clientThreadXXXX().  These typically have a mirrored methods named XXXX(), that execute on
 * the netty thread, and these paired methods redispatch the call onto the opposing pool.
 * <br/>
 * In order to wait for an insert to complete, this decoder toggles the netty channel autoRead on and off.  Since more data may
 * already be held by ByteToMessageDecoder and will be delivered even after the pause, we decoded normally and hold extra messages in a queue
 * for future processing.  The load data input could finish as part of this process, and so processing this queue cannot be
 * done solely on the channelRead() / decode() calls.  To keep things clean, other than the actual decoding, all state is
 * processed in a single loop (that exits when no more data is available/expected), inside the method processQueuedOutput().
 * <br/>
 * The ByteToMessageDecoder also holds on to any extra data that could not be decoded into a full frame, and when netty
 * delivers more data off the socket, the decoder will append the two together and ask us to decode the new bigger frame.  If
 * netty has already delivered the final bytes to the decoder and we pause, there will be no more messages from netty to
 * notify the decoder it should try and decode what it is holding.  Because of this, when we unpause, we pass a zero length
 * buffer to the decoder to simulate the arrival of more data.
 *
 */

public class MSPLoadDataDecoder extends ByteToMessageDecoder {
    private ExecutorService clientExecutorService;
	private SSConnection ssCon;
    private ChannelHandlerContext ctx;

    MyLoadDataInfileContext myLoadDataInfileContext;

    Queue<ByteBuf> decodedFrameQueue = new LinkedList<>();
    boolean decodedEOF = false;
    boolean waitingForInsert = false;
    Throwable encounteredError = null;
    boolean paused = false;



	public MSPLoadDataDecoder(ExecutorService clientExecutorService, SSConnection ssCon, MyLoadDataInfileContext myLoadDataInfileContext) {
		this.clientExecutorService = clientExecutorService;
		this.ssCon = ssCon;
		this.myLoadDataInfileContext = myLoadDataInfileContext;
	}

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;

        //TODO: this decoder isn't sharable/stateless, so storing the context on the channel doesn't really make sense. -sgossard
        MyLoadDataInfileContext loadDataInfileCtx = MyLoadDataInfileContext.getLoadDataInfileContextFromChannel(ctx);
        if (loadDataInfileCtx == null) {
            MyLoadDataInfileContext.setLoadDataInfileContextOnChannel(ctx, myLoadDataInfileContext);
        }

    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        //TODO: Avoid discarding/mangling a query on remove in the (unlikely) case where the client pipelined a request. -sgossard
        closePreparedStatements(myLoadDataInfileContext);

        //now we are sure we won't be getting any more packets, so turn autoread back on.
        resumeInput(ctx);

        //just to be safe, we'll release any bytebufs we are still holding.
        while (!decodedFrameQueue.isEmpty()){
            ReferenceCountUtil.release(decodedFrameQueue.remove());
        }
    }

    @Override
	protected void decode(final ChannelHandlerContext ignored, ByteBuf in, List<Object> out) throws Exception {

        decodeAvailableInput(in);

        processQueuedOutput(ctx);
	}

    private void decodeAvailableInput(ByteBuf in) {
        //we always parse the input frames until we hit load data EOF, so the stream is recoverable.

        while(!decodedEOF) {
            //slice off the frame sequence number and the frame payload.
            ByteBuf frame = decodeNextFrame(in);

            boolean inputDidntHaveFullFrame = (frame == null);
            if (inputDidntHaveFullFrame)
                break;

            decodedEOF = (frame.readableBytes() <= 1);

            decodedFrameQueue.add(frame);
        }

    }


    private void processQueuedOutput(ChannelHandlerContext ctx) throws Exception {
        byte packetNumber = 0;
        for (;;){
            ByteBuf nextDecodedFrame = decodedFrameQueue.poll();

            if (nextDecodedFrame == null)
                break;

            try {
                if ( encounteredError == null ) {

                    packetNumber = nextDecodedFrame.readByte();
                    byte[] frameData = MysqlAPIUtils.readBytes(nextDecodedFrame);

                    final List<List<byte[]>> parsedRows = LoadDataBlockExecutor.processDataBlock(ctx, ssCon, frameData);

                    insertRequest(ctx, parsedRows);

                    waitingForInsert = true;

                    if (waitingForInsert)
                        break; //OK, we sent out an insert. We'll wait for it to come back before sending another.
                }
            } catch (Throwable t) {
                failLoadData(t);
            } finally {
                ReferenceCountUtil.release(nextDecodedFrame);
            }
        }

        if (waitingForInsert) {
            pauseInput(ctx);
            return;
        }

        boolean loadDataIsFinished = decodedEOF || (encounteredError != null);

        if (loadDataIsFinished) {
            sendResponseAndRemove(ctx);
        } else {
            //not waiting for input, haven't seen input EOF or thrown an exception, must need more input data.
            resumeInput(ctx);
        }

    }

    private void pauseInput(ChannelHandlerContext ctx) {
        paused = true;
        //NOTE: may still get some data left in the decoder, which is why we have the decode queue.
        ctx.channel().config().setAutoRead(false);
    }

    private void resumeInput(ChannelHandlerContext ctx) {
        if (paused){ //make sure we don't recurse infinitely.
            paused = false;
            ctx.channel().config().setAutoRead(true);
            ctx.pipeline().fireChannelRead(Unpooled.EMPTY_BUFFER); //this flushes any partial packets held upstream (may cause recursion).
        }
    }

    private void sendResponseAndRemove(ChannelHandlerContext ctx)  {
        ChannelPipeline pipeline = ctx.pipeline();
        try {
            pauseInput(ctx);//stop incoming packets so we don't process the next request, we'll resume in the removal callback.

            MyMessage response;

            if (encounteredError == null)
                response = createLoadDataEOFMsg(myLoadDataInfileContext);
            else
                response = new MyErrorResponse(new PEException(encounteredError));

            pipeline.writeAndFlush(response);
            pipeline.remove(this);

        } catch (Exception e){
            ctx.channel().close();
        }
    }

    private ByteBuf decodeNextFrame(ByteBuf in){
        if (in.readableBytes() < 4) {  //three bytes for the length, one for the sequence.
            return null;
        }

        in.markReaderIndex();

        ByteBuf buffer = in.order(ByteOrder.LITTLE_ENDIAN);
        int length = buffer.readUnsignedMedium();

        if (buffer.readableBytes() < length + 1) {
            in.resetReaderIndex();
            return null;
        }

        return buffer.readSlice(length + 1).order(ByteOrder.LITTLE_ENDIAN).retain();
    }

    private void failLoadData(Throwable e) {
        encounteredError = e;
    }

    private void insertRequest(final ChannelHandlerContext ctx, final List<List<byte[]>> parsedRows) {
        clientExecutorService.submit(new Callable<Void>() {
            public Void call() throws Exception {
                return clientThreadInsertRequest(ctx, parsedRows);
            }
        });
    }

    private Void clientThreadInsertRequest(final ChannelHandlerContext ctx, final List<List<byte[]>> parsedRows) throws Exception {
        ssCon.executeInContext(new Callable<Void>() {
            public Void call() {
                try {
                    LoadDataBlockExecutor.executeInsert(ctx, ssCon, parsedRows);
                    clientThreadInsertSuccess(ctx);
                } catch (Throwable e) {
                    clientThreadInsertFailure(ctx, e);
                }
                return null;
            }
        });
        return null;
    }

    private void clientThreadInsertSuccess(final ChannelHandlerContext ctx) {

        ctx.executor().submit( new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                insertSuccess(ctx);
                return null;
            }
        });
    }

    private void clientThreadInsertFailure(final ChannelHandlerContext ctx, final Throwable e) {
        ctx.executor().submit( new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                insertFailure(ctx, e);
                return null;
            }
        });

    }

    private void insertSuccess(ChannelHandlerContext ctx) throws Exception {
        waitingForInsert = false;
        processQueuedOutput(ctx);//forward more frames, if available.
    }

    private void insertFailure(ChannelHandlerContext ctx, Throwable e) throws Exception {
        failLoadData(e);
        processQueuedOutput(ctx);
    }

    private void closePreparedStatements(MyLoadDataInfileContext loadDataInfileCtx) {
		for (MyPreparedStatement<String> pStmt : loadDataInfileCtx.getPreparedStatements()) {
			ssCon.removePreparedStatement(pStmt.getStmtId());
			QueryPlanner.destroyPreparedStatement(ssCon, pStmt.getStmtId());
		}
		loadDataInfileCtx.clearPreparedStatements();
	}

	static public MyOKResponse createLoadDataEOFMsg(MyLoadDataInfileContext loadDataInfileCtx) {
        MyOKResponse okResp = new MyOKResponse();
		okResp.setAffectedRows(loadDataInfileCtx.getInfileRowsAffected());
		okResp.setWarningCount((short) loadDataInfileCtx.getInfileWarnings());
		okResp.setInsertId(0);
		okResp.setStatusInTrans(false);

		return okResp;
	}

}
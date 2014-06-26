package com.tesora.dve.db.mysql.portal;

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

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.mysql.MysqlNativeCharSet;
import com.tesora.dve.clock.*;
import com.tesora.dve.common.PECharsetUtils;
import com.tesora.dve.db.mysql.MysqlLoadDataInfileRequestCollector;
import com.tesora.dve.db.mysql.portal.protocol.*;
import com.tesora.dve.server.connectionmanager.loaddata.LoadDataRequestExecutor;
import com.tesora.dve.server.connectionmanager.loaddata.MSPLoadDataDecoder;
import com.tesora.dve.singleton.Singletons;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPCommandHandler extends ChannelInboundHandlerAdapter {
	private static final Logger logger = Logger.getLogger(MSPCommandHandler.class);

	private static boolean processUnhandledExceptions = true;

	private final static MSPAction mspActions[] = {
			MSPComQueryRequest.INSTANCE,
			MSPComFieldListRequest.INSTANCE,
			MSPComQuitRequest.INSTANCE,
			MSPComSetOptionRequest.INSTANCE,
			MSPComPingRequest.INSTANCE,
			MSPComInitDBRequest.INSTANCE,
			MSPComPrepareStmtRequest.INSTANCE,
			MSPComStmtExecuteRequest.INSTANCE,
			MSPComStmtCloseRequest.INSTANCE,
			MSPComProcessInfoRequest.INSTANCE,
			MSPComStatisticsRequest.INSTANCE
	};

	private final static MSPAction[] executorMap = new MSPAction[256];
	static {
		for(MSPAction m : mspActions)
			executorMap[m.getMysqlMessageType()] = m;
	}

    TimingService timingService = Singletons.require(TimingService.class, NoopTimingService.SERVICE);
    enum TimingDesc {FRONTEND_ROUND_TRIP}

    private MSPAction[] instanceExecutor;
	private ExecutorService clientExecutorService;
	
	public MSPCommandHandler(ExecutorService clientExecutorService, MSPAction[] instanceExecutor) throws PEException {
		this.instanceExecutor = instanceExecutor;
		this.clientExecutorService = clientExecutorService;
	}

	public MSPCommandHandler(ExecutorService clientExecutorService) throws PEException {
		this(clientExecutorService, executorMap);
	}

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        final SSConnection ssCon = ctx.channel().attr(ConnectionHandlerAdapter.SSCON_KEY).get();
        try {
            if (!(msg instanceof MSPMessage)){
                ctx.fireChannelRead(msg); //not for us, maybe someone further in the stack can handle it.
                return;
            }

            //we start the timer here, outside the submit/callable, so that we include any delay in submission/execution around the thread pool.
            final Timer frontendRequest = timingService.startSubTimer(TimingDesc.FRONTEND_ROUND_TRIP);

            final MSPMessage mspMessage = (MSPMessage)msg;
            final byte theMessageType = mspMessage.getMysqlMessageType();

            clientExecutorService.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    ssCon.executeInContext(new Callable<Void>() {
                        public Void call()  {
                            //bind the frontend timer to this thread, so that new sub-timers on this thread (planning, backend, etc ) will be children of the frontend request timer.
                            timingService.attachTimerOnThread(frontendRequest);
                            try {
                                MSPAction mspAction = instanceExecutor[theMessageType];
                                //TODO:need to get load data to play nice, this special casing violates the MSPAction abstraction and the copy/regex runs for every statement, even though 'load data' is uncommon. -sgossard
                                if (mspMessage instanceof MSPComQueryRequestMessage && (isLoadDataStmt((MSPComQueryRequestMessage)mspMessage)) ){
                                    MSPComQueryRequestMessage queryMessage = (MSPComQueryRequestMessage)mspMessage;
                                    executeLoadDataStatement(clientExecutorService, ctx,ssCon, queryMessage );
                                } else {
                                    mspAction.execute(clientExecutorService, ctx, ssCon, mspMessage);
                                }
                            } catch (Throwable t) {
                                ctx.fireExceptionCaught(t);
                            } finally {
                                frontendRequest.end();
                                timingService.detachTimerOnThread();
                            }
                            return null;
                        }
                    });
                    return null;
                }
            });
        } finally {
            PEThreadContext.clear();
        }
    }


    static boolean isLoadDataStmt(MSPComQueryRequestMessage message) {
        ByteBuf rawStatement = message.getQueryNative();//returns a slice()
        try{
            //previous code made two copies of the query data before we even get to the parser, direct buf ==> byte[] ==> String
            //the parser requires a byte[] array, so (for now) we *must* do direct buf ==> byte[]
            //TODO: this code does direct buf ==> CharSequence, but a custom CharSequence could peek into the byte[], avoiding most of one full copy. -sgossard
            CharsetDecoder decoder = PECharsetUtils.latin1.newDecoder();
            CharBuffer charBuf = decoder.decode(rawStatement.nioBuffer());
            return MSPComQueryRequest.IS_LOAD_DATA_STATEMENT.matcher(charBuf).matches();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("query statement does not appear to be latin1");
        }
    }

    static void executeLoadDataStatement(ExecutorService clientExecutorService, ChannelHandlerContext ctx, SSConnection ssCon,MSPComQueryRequestMessage queryMessage) throws PEException {
        byte sequenceId = queryMessage.getSequenceID();
        byte[] query = queryMessage.getQueryBytes();
        NativeCharSet clientCharSet = MysqlNativeCharSet.UTF8;
        MysqlLoadDataInfileRequestCollector resultConsumer = new MysqlLoadDataInfileRequestCollector(ctx, sequenceId);
        try {
            LoadDataRequestExecutor.execute(ctx, ssCon, resultConsumer, clientCharSet.getJavaCharset(), query);
            if (resultConsumer.getLoadDataInfileContext().isLocal()) {
                ctx.pipeline().addFirst(MSPLoadDataDecoder.class.getSimpleName(), new MSPLoadDataDecoder(clientExecutorService, ssCon, resultConsumer.getLoadDataInfileContext()));
                resultConsumer.sendStartDataRequest();
            }
        } catch (PEException e) {
            if (MSPComQueryRequest.logger.isInfoEnabled())
                MSPComQueryRequest.logger.info("Exception returned to user: ", e);
            resultConsumer.sendError(e);
            if (ctx.pipeline().get(MSPLoadDataDecoder.class.getSimpleName()) != null) {
                ctx.pipeline().remove(MSPLoadDataDecoder.class.getSimpleName());
            }
        } catch (Throwable t) {
            if (MSPComQueryRequest.logger.isInfoEnabled())
                MSPComQueryRequest.logger.info("Exception returned to user: ", t);
            resultConsumer.sendError(new Exception(t));
            if (ctx.pipeline().get(MSPLoadDataDecoder.class.getSimpleName()) != null) {
                ctx.pipeline().remove(MSPLoadDataDecoder.class.getSimpleName());
            }
        }
    }

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		if (processUnhandledExceptions && ctx.channel().isActive()) {
			logger.warn(ctx.handler().toString() + " - Unexpected exception from downstream.", cause);
			Exception exc;
			boolean closeChannel = false;
			if (cause instanceof Exception) {
				exc = (Exception) cause;
			} else {
				closeChannel = true;
				exc = new Exception("Unhandled Exception", cause);
			}
			ctx.write(new MyErrorResponse(exc));
			if (closeChannel)
				ctx.close();
		}
    }

}

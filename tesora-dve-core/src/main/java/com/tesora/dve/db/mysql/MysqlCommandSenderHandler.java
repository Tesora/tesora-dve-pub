package com.tesora.dve.db.mysql;

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

import com.tesora.dve.charset.NativeCharSetCatalog;
import com.tesora.dve.clock.*;
import com.tesora.dve.common.DBType;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.portal.protocol.MysqlClientAuthenticationHandler;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.singleton.Singletons;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import io.netty.util.ReferenceCountUtil;
import org.apache.log4j.Logger;

public class MysqlCommandSenderHandler extends ChannelDuplexHandler {

	private static final Logger logger = Logger.getLogger(MysqlCommandSenderHandler.class);
    final String socketDesc;
    DBConnection.Monitor monitor;
    TimingService timingService = Singletons.require(TimingService.class, NoopTimingService.SERVICE);

    public MysqlCommandSenderHandler(StorageSite site, DBConnection.Monitor monitor) {
        this.socketDesc = site.getName();
        this.monitor = monitor;
    }

    enum TimingDesc {BACKEND_ROUND_TRIP, BACKEND_RESPONSE_PROCESSING}

    MysqlCommand previousCommand = null;
	List<MysqlCommand> cmdList = new LinkedList<MysqlCommand>();

	Charset serverCharset = null;

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (!(msg instanceof MysqlCommand)){
            logger.warn("Don't know how to handle message, passing downstream :" + (msg) );
            ctx.write(msg); //see if someone downstream can handle this.
            return;
        }

		MysqlCommand cast = (MysqlCommand) msg;

        if (logger.isDebugEnabled())
            logger.debug(ctx.channel() + " flush rec'd cmd " + cast);

        Timer commandTimer = cast.frontendTimer.newSubTimer(TimingDesc.BACKEND_ROUND_TRIP);
        cast.commandTimer = commandTimer;
        Timer previouslyAttached = timingService.attachTimerOnThread(cast.frontendTimer);
        boolean noResponse = false;
        try {
            noResponse = cast.isDone(ctx);//finished before we started, no responses will get processed.

            cmdList.add(cast);

            cast.executeInContext(monitor,ctx, getServerCharset(ctx));

            if (noResponse){
                commandTimer.end(
                    cast.getClass().getName()
                );
            }

            lookupActiveCommand(ctx);//quick check to see if request is already done, (IE stmt close)
        } catch (Exception e) {
            logger.error("Connection " + ctx.channel() + "to " + ctx.channel().remoteAddress()
                    + " closed due to exception", e);
            ctx.close();
            cast.failure(e);
        } finally {
            timingService.attachTimerOnThread(previouslyAttached);
        }

    }

	private Charset getServerCharset(ChannelHandlerContext ctx) {
		if (serverCharset == null)
			serverCharset = ctx.channel().attr(MysqlClientAuthenticationHandler.HANDSHAKE_KEY).get().getServerCharset( NativeCharSetCatalog.getDefaultCharSetCatalog(DBType.MYSQL) );
		return serverCharset;
	}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MyMessage){
            MyMessage message = (MyMessage)msg;
            dispatch(ctx,message);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("unexpected message type, %s", (msg == null ? "null" : msg.getClass().getName())));
            }
            ctx.fireChannelRead(msg);
        }

    }

    protected void dispatch(ChannelHandlerContext ctx, MyMessage message) throws Exception {
        MysqlCommand cmd = null;
        Timer responseProcessing = null;
        try {
            cmd = lookupActiveCommand(ctx);
            if (cmd == null){
                logger.warn(String.format("Received message %s, but no active command registered, discarding.", message.getClass().getName()));
                ReferenceCountUtil.release(message);
                return;
            }

            cmd.incrementResultsProcessedCount();

            if (logger.isDebugEnabled() && cmd.resultsProcessedCount() == 1)
                logger.debug(ctx.channel() + ": results received for cmd " + cmd);

            responseProcessing = cmd.commandTimer.newSubTimer(TimingDesc.BACKEND_RESPONSE_PROCESSING);
            timingService.attachTimerOnThread(cmd.commandTimer);
            cmd.processPacket(ctx, message);
            responseProcessing.end(
                    socketDesc,
                    cmd.getClass().getName()
            );
            lookupActiveCommand(ctx);//fast triggers removal of finished commands to get accurate completion time.

        } catch (PEException e) {
            cmd.failure(e);
        } catch (Exception e) {
            String errorMsg = String.format("encountered problem processing %s via %s, failing command.\n", (message.getClass().getName()), (cmd == null ? "null" : cmd.getClass().getName()));
            if (cmd==null || logger.isDebugEnabled())
                logger.warn(errorMsg,e);
            else
                logger.warn(errorMsg);
            if (cmd != null)
                cmd.failure(e);
        } finally {
            timingService.detachTimerOnThread();
            if (responseProcessing != null)
                responseProcessing.end();
        }
    }

    protected MysqlCommand lookupActiveCommand(ChannelHandlerContext ctx) {
        for (;;){
            if (cmdList.isEmpty()){
                return null;
            }
            MysqlCommand cmd = cmdList.get(0);
            if (previousCommand != cmd){
                previousCommand = cmd;
                cmd.active(ctx);
            }
            if (cmd.isDone(ctx)) {
                cmd.commandTimer.end( socketDesc,
                        cmd.getClass().getName()
                );

                cmdList.remove(0);
                if (logger.isDebugEnabled())
                    logger.debug(ctx.channel() + ": "+cmd.resultsProcessedCount()+" results received for deregistered cmd " + cmd);

                continue;
            } else {
                return cmd;
            }
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if ( ! cmdList.isEmpty() ){
            cmdList.get(0).packetStall(ctx);
        }
        super.channelReadComplete(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (!cmdList.isEmpty()) {
            //premature closure, we had outstanding commands.
            failAllCommands(ctx, null);
        }
        super.channelInactive(ctx);
    }



    private void failAllCommands(ChannelHandlerContext ctx, Exception cause) {
        if (!cmdList.isEmpty()) {
            for (MysqlCommand cmd : cmdList) {
                PEException communicationsFailureException =
                        new PECommunicationsException("Connection closed before completing command: " + cmd);
                if (cause != null)
                    communicationsFailureException.initCause(cause);
                cmd.failure(communicationsFailureException);
            }
        }
    }

}

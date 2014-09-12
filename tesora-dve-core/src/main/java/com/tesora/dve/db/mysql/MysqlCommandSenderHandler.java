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

    boolean sentActiveEventToHeadOfQueue = false;
	LinkedList<MysqlCommand> cmdList = new LinkedList<>();

	Charset serverCharset = null;

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (!(msg instanceof MysqlCommand)){
            logger.warn("Don't know how to handle message, passing downstream :" + (msg) );
            ctx.write(msg);
            return;
        }
		MysqlCommand cast = (MysqlCommand) msg;

        if (logger.isDebugEnabled())
            logger.debug(ctx.channel() + " flush rec'd cmd " + cast);


        Timer commandTimer = cast.frontendTimer.newSubTimer(TimingDesc.BACKEND_ROUND_TRIP);
        cast.commandTimer = commandTimer;
        Timer previouslyAttached = timingService.attachTimerOnThread(cast.frontendTimer);

        try {

            dispatchWrite(ctx, cast, commandTimer);

        } catch (Exception e) {
            logger.error("Connection " + ctx.channel() + "to " + ctx.channel().remoteAddress()
                    + " closed due to exception", e);
            ctx.close();
            cast.failure(e);
        } finally {
            timingService.attachTimerOnThread(previouslyAttached);
        }
    }

    private void dispatchWrite(ChannelHandlerContext ctx, MysqlCommand command, Timer commandTimer) throws PEException {

        //ask the command to write the messages on the socket (they'll be sent out when we return).
        command.executeInContext(monitor, ctx, getServerCharset(ctx));

        if (command.isExpectingResults(ctx)) { //TODO: this should move onto the protocol message. -sgossard
            //add it to the command deque , so we can route responses back to it.
            enqueueCommand(command);
        } else {
            //no response expected, so this command is done early.  Fire all the lifecycle stuff now.
            command.active(ctx);
            commandTimer.end(
                command.getClass().getName()
            );
        }

    }

    private Charset getServerCharset(ChannelHandlerContext ctx) {
        //TODO: can't this just be statically bound? Looking it up off the channel attributes feels dirty.-sgossard
		if (serverCharset == null)
			serverCharset = ctx.channel().attr(MysqlClientAuthenticationHandler.HANDSHAKE_KEY).get().getServerCharset( NativeCharSetCatalog.getDefaultCharSetCatalog(DBType.MYSQL) );
		return serverCharset;
	}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MyMessage){
            MyMessage message = (MyMessage)msg;
            dispatchRead(ctx, message);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("unexpected message type, %s", (msg == null ? "null" : msg.getClass().getName())));
            }
            ctx.fireChannelRead(msg);
        }

    }

    protected void dispatchRead(ChannelHandlerContext ctx, MyMessage message) throws Exception {
        boolean messageSignalsEndOfRequest = message.isSequenceEnd();

        MysqlCommand activeCommand = null;
        Timer responseProcessing = null;
        try {

            activeCommand = activateFirstCommandIfNeeded(ctx);

            if ( activeCommand == null ) {
                logger.warn(String.format("Received message %s, but no active command registered, discarding.", message.getClass().getName()));
                ReferenceCountUtil.release(message);
                return;
            }

            activeCommand.incrementResultsProcessedCount();

            if (logger.isDebugEnabled() && activeCommand.resultsProcessedCount() == 1)
                logger.debug(ctx.channel() + ": results received for cmd " + activeCommand);

            responseProcessing = activeCommand.commandTimer.newSubTimer(TimingDesc.BACKEND_RESPONSE_PROCESSING);
            timingService.attachTimerOnThread(activeCommand.commandTimer);

            activeCommand.processPacket(ctx, message);

            responseProcessing.end(
                    socketDesc,
                    activeCommand.getClass().getName()
            );

        } catch (PEException e) {
            activeCommand.failure(e);
        } catch (Exception e) {
            String errorMsg = String.format("encountered problem processing %s via %s, failing command.\n", (message.getClass().getName()), (activeCommand == null ? "null" : activeCommand.getClass().getName()));
            if (activeCommand==null || logger.isDebugEnabled())
                logger.warn(errorMsg,e);
            else
                logger.warn(errorMsg);
            if (activeCommand != null)
                activeCommand.failure(e);
        } finally {
            if (messageSignalsEndOfRequest) {
                popActiveCommand(ctx);
                activateFirstCommandIfNeeded(ctx);
            }
            timingService.detachTimerOnThread();
            if (responseProcessing != null)
                responseProcessing.end();
        }
    }

    private void enqueueCommand(MysqlCommand command) {
        cmdList.addLast(command);
    }

    private void popActiveCommand(ChannelHandlerContext ctx) {
        sentActiveEventToHeadOfQueue = false;
        MysqlCommand cmd = cmdList.pollFirst();
        if (cmd != null && logger.isDebugEnabled())
            logger.debug(ctx.channel() + ": " + cmd.resultsProcessedCount() + " results received for deregistered cmd " + cmd);
    }

    private MysqlCommand activateFirstCommandIfNeeded(ChannelHandlerContext ctx) {
        MysqlCommand cmd = cmdList.peekFirst();
        if (cmd != null && !sentActiveEventToHeadOfQueue){
            sentActiveEventToHeadOfQueue = true;
            cmd.active(ctx); //command is getting it's first response packet.
        }
        return cmd;
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
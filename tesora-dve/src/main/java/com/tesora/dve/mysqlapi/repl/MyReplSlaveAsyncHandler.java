package com.tesora.dve.mysqlapi.repl;

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

import com.tesora.dve.mysqlapi.repl.messages.MyReplEvent;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyEOFPktResponse;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.mysqlapi.MyClientConnectionContext;
import com.tesora.dve.mysqlapi.MyClientConnectionHandler;

public class MyReplSlaveAsyncHandler extends MyClientConnectionHandler {
	static final Logger logger = Logger.getLogger(MyReplSlaveAsyncHandler.class);
	protected MyReplicationSlaveService plugin;

	public MyReplSlaveAsyncHandler(MyClientConnectionContext clientContext, MyReplicationSlaveService plugin) {
		super(clientContext);
		this.plugin = plugin;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, MyMessage msg,
			List<Object> out) throws Exception {
		if (plugin.stopCalled()) {
			// don't process any more messages
			return;
		}
		
		if (msg instanceof MyEOFPktResponse) {
			// TODO we need to implement retry logic - this means that the master went away
			if (logger.isDebugEnabled())
				logger.debug("EOF packet received from master");
			
			// we need to instrument this a little for the Repl Slave IT test
			// if we get an EOF pkt with -1 for status and warningCount we
			// are going to stop the slave (this helps the test)
			// For real use - we don't actually understand the real reason
			// this packet comes down so we are going to ignore it
			MyEOFPktResponse eofPkt = (MyEOFPktResponse) msg;
			if ( eofPkt.getStatusFlags() == -1 && eofPkt.getWarningCount() == -1 )
				plugin.stop();
		} else if (msg instanceof MyErrorResponse) {
			logger.error("Error received from master: "
					+ ((MyErrorResponse) msg).getErrorMsg());
		} else if (msg instanceof MyReplEvent) {
			((MyReplEvent) msg).processEvent(plugin);
		}
		return;
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if (logger.isDebugEnabled()) logger.debug("Channel closed.");
		
		if (!plugin.stopCalled()) {
			// if the plugin stop called is false then
			// it means the user or an error has not initiated
			// the closing of the connection so that means the
			// master has gone away
			plugin.restart();
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		if ( plugin.stopCalled() ) {
			// if the plugin is shutting down, ignore the exception and return
			return;
		}
	}
	
	
}

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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.messages.MyComBinLogDumpRequest;

public class MyServerSlaveRegisterHandler extends
		MessageToMessageDecoder<MyMessage> {

	private static final Logger logger = Logger
			.getLogger(MyServerLoginHandler.class);

	MyReplicationSlaveService plugin;
	MyReplSlaveClientConnection myClient;

	public MyServerSlaveRegisterHandler(MyReplicationSlaveService plugin,
			MyReplSlaveClientConnection myClient) {
		this.plugin = plugin;
		this.myClient = myClient;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, MyMessage msg,
			List<Object> out) throws Exception {
		try {
			onSlaveRegister(ctx, msg);
		} finally {
			ctx.pipeline().remove(this);
		}

	}

	void onSlaveRegister(ChannelHandlerContext ctx, MyMessage rm)
			throws PEException {
		if (!(((MyResponseMessage) rm).isOK())) {
			myClient.stop();
			throw new PEException(((MyErrorResponse) rm).getErrorMsg());
		}

		if (logger.isDebugEnabled())
			logger.debug("Slave registration successful with "
					+ plugin.getMasterLocator() + " for Slave #"
					+ plugin.getSlaveServerID());

		MyBinLogPosition blp;
		try {
			blp = plugin.getBinLogPosition();
		} catch (SQLException e) {
			myClient.stop();
			throw new PEException("Cannot find binlog_status information for "
					+ plugin.getMasterHost(), e);
		}

		// switch to Asynchronous messaging mode
		myClient.useAsynchMode();
		if (logger.isDebugEnabled())
			logger.debug("Sending binlog dump request to "
					+ plugin.getMasterLocator() + " for "
					+ plugin.getSlaveInfo());

		MyComBinLogDumpRequest blr = new MyComBinLogDumpRequest(
				plugin.getSlaveServerID(), blp.getPosition(), blp.getFileName());
		ctx.writeAndFlush(blr);
	}

}

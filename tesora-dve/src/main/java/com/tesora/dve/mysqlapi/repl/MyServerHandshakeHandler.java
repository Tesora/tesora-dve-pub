// OS_STATUS: public
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

import com.tesora.dve.db.mysql.portal.protocol.MSPAuthenticateV10MessageMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.db.mysql.libmy.MyHandshakeV10;
import com.tesora.dve.db.mysql.libmy.MyLoginRequest;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.db.mysql.portal.protocol.ClientCapabilities;

public class MyServerHandshakeHandler extends MessageToMessageDecoder<MyMessage> {
	private static final Logger logger = Logger.getLogger(MyServerHandshakeHandler.class);

	MyReplicationSlaveService plugin;
	MyReplSlaveClientConnection myClient;

	public MyServerHandshakeHandler(MyReplicationSlaveService plugin, MyReplSlaveClientConnection myClient) {
		this.plugin = plugin;
		this.myClient = myClient;
	}
	
	@Override
	protected void decode(ChannelHandlerContext ctx, MyMessage msg,
			List<Object> out) throws Exception {
		try {
			onHandshake(ctx, msg);
		} finally {
			ctx.pipeline().remove(MyServerHandshakeHandler.class.getSimpleName());
		}
	}

	private void onHandshake(ChannelHandlerContext ctx, MyMessage rm)
			throws PEException {
		if (rm instanceof MyHandshakeV10) {
			if (logger.isDebugEnabled()) {
				logger.debug("Received Server Greeting Response for "
						+ plugin.getName() + " from "
						+ plugin.getMasterLocator());
			}
			myClient.getContext().setSgr((MyHandshakeV10) rm);
		} else {
			// myClient.stop();
			throw new PEException(
					"Invalid state - expected MyServerGreetingResponse recieved "
							+ rm.getClass());
		}

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Sending login request to "
						+ plugin.getMasterLocator() + " for "
						+ plugin.getName() + " as " + plugin.getMasterUserid());
			}
			loginRequest(ctx, plugin.getMasterUserid(), plugin.getMasterPwd());
		} catch (Exception e) {
			// myClient.stop();
			throw new PEException("Error logging in to master as "
					+ plugin.getMasterUserid() + " at "
					+ plugin.getMasterLocator(), e);
		}
	}

	void loginRequest(ChannelHandlerContext ctx, String userid, String password)
			throws NoSuchAlgorithmException, UnsupportedEncodingException, PEException {
		long clientCapabilities = ClientCapabilities.CLIENT_LONG_PASSWORD
				+ ClientCapabilities.CLIENT_LONG_FLAG
				+ ClientCapabilities.CLIENT_LOCAL_FILES
				+ ClientCapabilities.CLIENT_PROTOCOL_41
				+ ClientCapabilities.CLIENT_TRANSACTIONS
				+ ClientCapabilities.CLIENT_SECURE_CONNECTION;
		String hashedPwd = MSPAuthenticateV10MessageMessage.computeSecurePasswordString(
                password, myClient.getContext().getSaltforPassword());

		ctx.pipeline().addLast(MyServerLoginHandler.class.getSimpleName(),
				new MyServerLoginHandler(plugin, myClient));

		MyLoginRequest lr = new MyLoginRequest(userid, hashedPwd)
				.setClientCharset(MysqlNativeConstants.MYSQL_CHARSET_LATIN1)
				.setMaxPacketSize(MysqlNativeConstants.MAX_PACKET_SIZE)
				.setClientCapabilities(clientCapabilities)
				.setPlugInData(myClient.getContext().getPlugInData());

		try {
			ctx.writeAndFlush(lr).sync();
		} catch (InterruptedException e) {
			throw new PEException("Cannot write login request.", e);
		}
	} 

}

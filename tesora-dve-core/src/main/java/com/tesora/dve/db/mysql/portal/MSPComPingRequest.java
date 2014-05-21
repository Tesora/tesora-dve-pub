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

import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPComPingRequest extends MSPActionBase {

	final static MSPComPingRequest INSTANCE = new MSPComPingRequest();
	
	private static final Logger logger = Logger.getLogger(MSPComPingRequest.class);

	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
		if (logger.isDebugEnabled())
			logger.debug(ctx.channel().toString() + " - Ping request recieved - returning OK response");

		ctx.channel().write(new MyOKResponse());
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x0e;
	}

}

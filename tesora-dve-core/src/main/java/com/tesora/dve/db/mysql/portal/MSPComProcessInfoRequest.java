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

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPComProcessInfoRequest extends MSPActionBase {
	
	final static MSPComProcessInfoRequest INSTANCE = new MSPComProcessInfoRequest(); 

	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
		MyErrorResponse errMsg = new MyErrorResponse(new PEException(
				"COM_PROCESS_INFO message has been deprecated - use SHOW PROCESSLIST"));
		ctx.channel().write(errMsg);
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x0a;
	}

}

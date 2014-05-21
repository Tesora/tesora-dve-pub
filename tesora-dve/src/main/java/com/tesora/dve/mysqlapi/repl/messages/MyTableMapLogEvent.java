// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyTableMapLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyTableMapLogEvent.class);

	int tableId;
	short reserved;
	ByteBuf variableData; 
	
	public MyTableMapLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		tableId = cb.readInt();
		reserved = cb.readShort();
		// TODO: need to parse out the variable part of the data
		variableData = Unpooled.buffer(cb.readableBytes());
		variableData.writeBytes(cb);
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeInt(tableId);
		cb.writeShort(reserved);
		cb.writeBytes(variableData);
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) {
		logger.warn("Message is parsed but no handler is implemented for log event type: TABLE_MAP_EVENT");
	}
}

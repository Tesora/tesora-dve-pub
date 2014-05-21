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

public class MyBeginLoadLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyBeginLoadLogEvent.class);

	int fileId;
	ByteBuf dataBlock;
	
	public MyBeginLoadLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		fileId = cb.readInt();
		dataBlock = Unpooled.buffer(cb.readableBytes());
		dataBlock.writeBytes(cb);
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeInt(fileId);
		cb.writeBytes(dataBlock);
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) throws PEException {
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("** START BeginLoadLog Event **");
				logger.debug("File id = " + fileId);
				logger.debug("** END BeginLoadLog Event **");
			}
			
			plugin.getInfileHandler().createInfile(fileId);
			plugin.getInfileHandler().addInitialBlock(fileId, dataBlock.array());
			
			updateBinLogPosition(plugin);

		} catch (Exception e) {
			throw new PEException("Receive BEGIN_LOAD_QUERY_EVENT but cannot create new infile.", e);
		}
	}
}

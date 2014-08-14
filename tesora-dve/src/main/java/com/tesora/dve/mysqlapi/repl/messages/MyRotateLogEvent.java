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
import io.netty.util.CharsetUtil;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyRotateLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger.getLogger(MyRotateLogEvent.class);

	long position;
	String newLogFileName;

	public MyRotateLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		position = cb.readLong();
		newLogFileName = cb.toString(CharsetUtil.UTF_8);
	}

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeLong(position);
		cb.writeBytes(newLogFileName.getBytes(CharsetUtil.UTF_8));
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) throws PEException {
		if (logger.isDebugEnabled()) {
			logger.debug("** START Rotate Event **");
			logger.debug("Position: " + position);
			logger.debug("New Log File: " + newLogFileName);
			logger.debug("** END Rotate Event **");
		}

		plugin.getSessionVariableCache().setRotateLogValue(newLogFileName);
		plugin.getSessionVariableCache().setRotateLogPositionValue(position);

		try {
			updateBinLogPosition(plugin);
		} catch (PEException e) {
			logger.error("Error updating binlog from Rotate Log Event.", e);
			// TODO I think we really need to stop the service in this case
			throw new PEException("Error updating bin log position",e);
		}
	}

	public long getPosition() {
		return position;
	}

	public void setPosition(long position) {
		this.position = position;
	}

	public String getNewLogFileName() {
		return newLogFileName;
	}

	public void setNewLogFileName(String logFileName) {
		this.newLogFileName = logFileName;
	}
}

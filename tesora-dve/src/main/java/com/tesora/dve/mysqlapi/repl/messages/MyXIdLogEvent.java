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

import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;

public class MyXIdLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger.getLogger(MyXIdLogEvent.class);

	long xid;
	static final String skipErrorMessage = "Replication Slave failed processing COMMIT but slave_skip_errors is active. Replication processing will continue";
	
	public MyXIdLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		xid = cb.readLong();
	}

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeLong(xid);
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) throws PEException {
		if ( logger.isDebugEnabled() )
			logger.debug("COMMIT (from XId event)");

		try {
			updateBinLogPosition(plugin);

			plugin.getServerDBConnection().executeUpdate("COMMIT");
		} catch (Exception e) {
			logger.error("Error occurred processing XID log event: ",e);
			try {
				plugin.getServerDBConnection().execute("ROLLBACK");
			} catch (SQLException e1) {
				throw new PEException("Error attempting to rollback after exception",e); // NOPMD by doug on 18/12/12 8:08 AM
			}
			throw new PEException("Error executing executing commit.",e);
		} finally { // NOPMD by doug on 18/12/12 8:08 AM
			// Clear all the session variables since they are only good for one
			// event
			plugin.getSessionVariableCache().clearAllSessionVariables();
		}

	}

	public long getXid() {
		return xid;
	}

	public void setXid(long xid) {
		this.xid = xid;
	}
	
	@Override
	public String getSkipErrorMessage() {
		if (!StringUtils.isBlank(skipErrorMessage)) {
			return skipErrorMessage;
		}
		return super.getSkipErrorMessage();
	}
}

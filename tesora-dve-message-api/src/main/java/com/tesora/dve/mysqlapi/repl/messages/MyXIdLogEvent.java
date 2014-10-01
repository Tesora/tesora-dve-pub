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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

public class MyXIdLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger.getLogger(MyXIdLogEvent.class);

	long xid;
	static final String skipErrorMessage = "Replication Slave failed processing COMMIT but slave_skip_errors is active. Replication processing will continue";
	
	public MyXIdLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

    @Override
    public void accept(ReplicationVisitorTarget visitorTarget) throws PEException {
        visitorTarget.visit((MyXIdLogEvent)this);
    }

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		xid = cb.readLong();
	}

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeLong(xid);
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

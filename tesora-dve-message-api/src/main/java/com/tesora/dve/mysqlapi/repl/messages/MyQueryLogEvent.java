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
import io.netty.util.CharsetUtil;

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.messages.MyStatusVariables.BaseQueryEvent;


public class MyQueryLogEvent extends MyLogEventPacket {
	static final Logger logger = Logger.getLogger(MyQueryLogEvent.class);

	long slaveProxyId;
	long execTime;
	byte dbNameLen;
	int errorCode;
	int statusVarsLen;
	String dbName;
	ByteBuf query;
	String origQuery;

	String skipErrorMessage;
	MyStatusVariables statusVars = new MyStatusVariables();
	
	public MyQueryLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

    @Override
    public void accept(ReplicationVisitorTarget visitorTarget) throws PEException {
        visitorTarget.visit((MyQueryLogEvent)this);
    }

	@Override
	public void unmarshallMessage(ByteBuf cb) throws PEException {
		
		// read event header
		slaveProxyId = cb.readUnsignedInt();
		execTime = cb.readUnsignedInt();
		dbNameLen = cb.readByte();
		errorCode = cb.readUnsignedShort();
		statusVarsLen = cb.readUnsignedShort();

		statusVars.parseStatusVariables(cb, statusVarsLen);
		
		dbName = MysqlAPIUtils.readBytesAsString(cb, dbNameLen, CharsetUtil.UTF_8);
		cb.skipBytes(1); //for trailing 0
		query = Unpooled.buffer(cb.readableBytes());
		query.writeBytes(cb);
		origQuery = query.toString(CharsetUtil.UTF_8);
	}

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeInt((int)getSlaveProxyId());
		cb.writeInt((int)getExecTime());
		cb.writeByte(getDbNameLen());
		cb.writeShort(getErrorCode());
		cb.writeShort(getStatusVarsLen());

		statusVars.writeStatusVariables(cb);
		
		cb.writeBytes(getDbName().getBytes(CharsetUtil.UTF_8));
		cb.writeByte(0); //for trailing 0
		cb.writeBytes(getQuery());
	}

	public long getSlaveProxyId() {
		return slaveProxyId;
	}

	public void setSlaveProxyId(long slaveProxyId) {
		this.slaveProxyId = slaveProxyId;
	}

	public long getExecTime() {
		return execTime;
	}

	public void setExecTime(long execTime) {
		this.execTime = execTime;
	}

	public byte getDbNameLen() {
		return dbNameLen;
	}

	public void setDbNameLen(byte dbNameLen) {
		this.dbNameLen = dbNameLen;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public int getStatusVarsLen() {
		return statusVarsLen;
	}

	public void setStatusVarsLen(int statusVarsLen) {
		this.statusVarsLen = statusVarsLen;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public ByteBuf getQuery() {
		return query;
	}

	public void setQuery(ByteBuf query) {
		this.query = query;
	}

	protected void setSuppliedEventCodes(Set<BaseQueryEvent> suppliedEventCodes) {
		this.statusVars.setSuppliedEventCodes(suppliedEventCodes);
	}

	@Override
	public String getSkipErrorMessage() {
		if (!StringUtils.isBlank(skipErrorMessage)) {
			return skipErrorMessage;
		}
		return super.getSkipErrorMessage();
	}

    public void setSkipErrors(boolean skip, String message) {
        super.setSkipErrors(skip);
        this.skipErrorMessage = message;
    }

    public String getOrigQuery() {
        return origQuery;
    }
}

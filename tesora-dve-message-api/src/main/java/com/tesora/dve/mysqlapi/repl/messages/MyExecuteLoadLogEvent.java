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

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PEException;

public class MyExecuteLoadLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyExecuteLoadLogEvent.class);

	public static final int MAX_BUFFER_LEN = 100000;
	int threadId;
	int time;
	byte dbLen;
	short errorCode;
	short statusBlockLen;
	int fileId;
	int startPos;
	int endPos;
	byte duplicateFlag;
	String dbName;
	ByteBuf query;
	String origQuery;
	String skipErrorMessage;

	MyStatusVariables statusVars = new MyStatusVariables();
	
	public MyExecuteLoadLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

    @Override
    public void accept(ReplicationVisitorTarget visitorTarget) throws PEException {
        visitorTarget.visit((MyExecuteLoadLogEvent)this);
    }

	@Override
	public void unmarshallMessage(ByteBuf cb) throws PEException {
		threadId = cb.readInt();
		time = cb.readInt();
		dbLen = cb.readByte();
		errorCode = cb.readShort();
		statusBlockLen = cb.readShort();
		fileId = cb.readInt();
		startPos = cb.readInt();
		endPos = cb.readInt();
		duplicateFlag = cb.readByte();
		// really we should check if replication version >=4 or else this is wrong
		statusVars.parseStatusVariables(cb, statusBlockLen);
		
		dbName = MysqlAPIUtils.readBytesAsString(cb, dbLen, CharsetUtil.UTF_8);
		cb.skipBytes(1); //for trailing 0
		query = Unpooled.buffer(cb.readableBytes());
		query.writeBytes(cb);
		origQuery = query.toString(CharsetUtil.UTF_8);
	}

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeInt(threadId);
		cb.writeInt(time);
		cb.writeByte(dbLen);
		cb.writeShort(errorCode);
		cb.writeShort(statusBlockLen);
		cb.writeInt(fileId);
		cb.writeInt(startPos);
		cb.writeInt(endPos);
		cb.writeByte(duplicateFlag);

		statusVars.writeStatusVariables(cb);
		
		cb.writeBytes(dbName.getBytes(CharsetUtil.UTF_8));
		cb.writeByte(0); //for trailing 0
		cb.writeBytes(query);
	}

    public String getDbName() {
        return dbName;
    }

    public String getOrigQuery() {
        return origQuery;
    }

    public int getFileId() {
        return fileId;
    }

	public short getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(short errorCode) {
		this.errorCode = errorCode;
	}

    public void setSkipErrors(boolean shouldSkip, String skipErrorMessage){
        this.setSkipErrors(shouldSkip);
        this.skipErrorMessage = skipErrorMessage;
    }

}

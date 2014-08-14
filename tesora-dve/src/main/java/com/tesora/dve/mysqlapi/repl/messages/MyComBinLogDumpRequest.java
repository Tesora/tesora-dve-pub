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

import com.tesora.dve.db.mysql.libmy.MyMessageType;
import com.tesora.dve.db.mysql.libmy.MyRequestMessage;

public class MyComBinLogDumpRequest extends MyRequestMessage {

	long binlogPosition;
	int slaveServerID;
	String binlogFileName=null;

	public MyComBinLogDumpRequest() {}

	public MyComBinLogDumpRequest(int slaveServerId, long binlogPosition, String binlogFileName) {
		this.slaveServerID = slaveServerId;
		this.binlogPosition = binlogPosition;
		this.binlogFileName = binlogFileName;
	}
	
	public long getBinlogPosition() {
		return binlogPosition;
	}

	public int getSlaveServerID() {
		return slaveServerID;
	}

	public String getBinlogFileName() {
		return binlogFileName;
	}

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeInt((int) binlogPosition);
		cb.writeZero(2); 	// binlog_flags
		cb.writeInt(slaveServerID);
		if ( binlogFileName != null ) {
			cb.writeBytes(binlogFileName.getBytes(CharsetUtil.UTF_8));
		}
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		binlogPosition = cb.readUnsignedInt();
		cb.skipBytes(2);
		slaveServerID = cb.readInt();
		if ( cb.readableBytes() > 0 ) {
			binlogFileName = cb.readSlice(cb.readableBytes()).toString(CharsetUtil.UTF_8);
		}
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.COM_BINLOG_DUMP_REQUEST;
	}
}

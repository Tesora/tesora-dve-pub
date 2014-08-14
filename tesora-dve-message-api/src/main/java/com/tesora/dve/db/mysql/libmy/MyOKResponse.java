package com.tesora.dve.db.mysql.libmy;

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

import java.nio.ByteOrder;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;

public class MyOKResponse extends MyResponseMessage {

	public static final byte OKPKT_INDICATOR = 0;
	
	private long affectedRows = 0;
	private long insertId = 0;
	private int serverStatus = MyProtocolDefs.SERVER_STATUS_AUTOCOMMIT;
	private int warningCount = 0;
	private String message;

    public MyOKResponse() {
        this.setPacketNumber((byte)1);//OK response is usually second packet in sequence.
    }

    @Override
    public void marshallMessage(ByteBuf in) {
		ByteBuf cb = in.order(ByteOrder.LITTLE_ENDIAN);
		cb.writeByte(0); // field_count - spec says this is always 0
		MysqlAPIUtils.putLengthCodedLong(cb, affectedRows);
		MysqlAPIUtils.putLengthCodedLong(cb, insertId);
		cb.writeShort(serverStatus);
		cb.writeShort(warningCount);
		if (message != null && message.length() > 0) {
			cb.writeBytes(message.getBytes());
		}
	}
	
	public int calculateSize() {
		return 23 + ((message != null) ? message.length() : 0);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		cb.readByte(); // field count - always 0
		affectedRows = MysqlAPIUtils.getLengthCodedLong(cb);
		insertId = MysqlAPIUtils.getLengthCodedLong(cb);
		serverStatus = cb.readUnsignedShort();
		warningCount = cb.readUnsignedShort();
		if (cb.isReadable())
			message = MysqlAPIUtils.readBytesAsString(cb, CharsetUtil.UTF_8);
	}
	
	public static long getAffectedRows(ByteBuf cb) {
		cb.readByte();
		return MysqlAPIUtils.getLengthCodedLong(cb);
	}

	public long getAffectedRows() {
		return affectedRows;
	}

	public void setAffectedRows(long affectedRows) {
		this.affectedRows = affectedRows;
	}

	public void setAffectedRows(int affectedRows) {
		this.affectedRows = (long) affectedRows;
	}

	public long getInsertId() {
		return insertId;
	}

	public void setInsertId(long insertId) {
		this.insertId = insertId;
	}

	public short getWarningCount() {
		return (short) warningCount;
	}

	public void setWarningCount(short warningCount) {
		this.warningCount = warningCount;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public short getServerStatus() {
		return (short) serverStatus;
	}

	public void setServerStatus(short serverStatus) {
		this.serverStatus = serverStatus;
	}

	public void setStatusInTrans(boolean inTransaction) {
		if ( inTransaction )
			serverStatus = (short) (serverStatus | MyProtocolDefs.SERVER_STATUS_IN_TRANS);
		else 
			serverStatus = (short) (serverStatus & ~MyProtocolDefs.SERVER_STATUS_IN_TRANS);
	}
	
	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.OK_RESPONSE;
	}
	
	@Override
	public String toString() {
		return super.toString() + " affectedRows=" + affectedRows + " insertId=" + insertId + " warningCount=" + warningCount;
	}


}

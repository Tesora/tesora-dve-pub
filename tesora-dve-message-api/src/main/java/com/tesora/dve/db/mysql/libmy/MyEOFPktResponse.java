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

import io.netty.buffer.ByteBuf;

public class MyEOFPktResponse extends MyResponseMessage {
	public static final byte EOFPKK_FIELD_COUNT = ((byte) 0xfe);
	private short warningCount;
	private short statusFlags;

	public MyEOFPktResponse() {
	}

	public MyEOFPktResponse(short statusFlags, short warningCount) {
		this.statusFlags = statusFlags;
		this.warningCount = warningCount;
	}

	@Override
	public void marshallMessage(ByteBuf cb) {
		cb.writeByte(EOFPKK_FIELD_COUNT);
		cb.writeShort(warningCount);
		cb.writeShort(statusFlags);
	}
	
	public int calculateSize() {
		return 5;
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		warningCount = cb.readShort();
		statusFlags = cb.readShort();
	}

	public short getWarningCount() {
		return warningCount;
	}

	public void setWarningCount(short warningCount) {
		this.warningCount = warningCount;
	}

	public short getStatusFlags() {
		return statusFlags;
	}

	public void setStatusFlags(short statusFlags) {
		this.statusFlags = statusFlags;
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.EOFPKT_RESPONSE;
	}

	@Override 
	public String toString() {
		return super.toString() + " warningCount=" + warningCount + " statusFlags=" + statusFlags;
	}

}

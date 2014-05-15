// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

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

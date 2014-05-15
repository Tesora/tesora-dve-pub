// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import io.netty.buffer.ByteBuf;

import com.tesora.dve.exceptions.PEException;

public class MyPrepareOKResponse extends MyResponseMessage {

	public static final byte OKPKT_FIELD_COUNT = 0;
	protected long stmtId;
	protected int warningCount=0;
	protected int numColumns=0;
	protected int numParams=0;
	
	public MyPrepareOKResponse() {
	}

    public MyPrepareOKResponse(MyPrepareOKResponse other) {
        this.setPacketNumber( other.getPacketNumber() );//copy packet sequence.
        this.stmtId = other.stmtId;
        this.warningCount = other.warningCount;
        this.numColumns = other.numColumns;
        this.numParams = other.numParams;
    }
	
	public MyPrepareOKResponse( int numParams, int numColumns ) {
		this.numParams = numParams;
		this.numColumns = numColumns;
	}
	
	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeZero(1);				// status
		cb.writeInt((int) stmtId);
		cb.writeShort(numColumns);
		cb.writeShort(numParams);
		cb.writeZero(1);				// filler
		cb.writeShort(warningCount);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		cb.skipBytes(1);
		stmtId = cb.readInt();
		numColumns = cb.readUnsignedShort();
		numParams = cb.readUnsignedShort();
		cb.skipBytes(1);
		warningCount = cb.readShort();
	}

	public long getStmtId() {
		return stmtId;
	}

	public void setStmtId(long stmtId) {
		this.stmtId = stmtId;
	}

	public int getWarningCount() {
		return warningCount;
	}

	public void setWarningCount(int warningCount) {
		this.warningCount = warningCount;
	}

	public int getNumColumns() {
		return numColumns;
	}

	public void setNumColumns(int numColumns) {
		this.numColumns = numColumns;
	}

	public int getNumParams() {
		return numParams;
	}

	public void setNumParams(int numParams) {
		this.numParams = numParams;
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.PREPAREOK_RESPONSE;
	}

}

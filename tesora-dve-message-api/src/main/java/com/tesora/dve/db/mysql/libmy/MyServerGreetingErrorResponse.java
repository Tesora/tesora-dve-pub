// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import io.netty.buffer.ByteBuf;

import com.tesora.dve.exceptions.PECodingException;

public class MyServerGreetingErrorResponse extends MyErrorResponse {

	public MyServerGreetingErrorResponse(Exception e) {
		super(e);
		setPacketNumber(0);	// packet number must be 0 for this message
	}

	@Override
	public void marshallMessage(ByteBuf cb) {
		cb.writeByte(ERRORPKT_FIELD_COUNT);
		cb.writeShort((short) getErrorNumber());
		cb.writeBytes(getErrorMsg().getBytes());
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.SERVER_GREETING_ERROR_RESPONSE;
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		throw new PECodingException("Method not supported for " + this.getClass().getName() );
	}
}

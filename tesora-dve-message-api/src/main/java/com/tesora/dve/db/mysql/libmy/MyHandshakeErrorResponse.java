// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import com.tesora.dve.exceptions.PEException;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public class MyHandshakeErrorResponse extends MyErrorResponse {
	
	Charset charset;

	public MyHandshakeErrorResponse(Exception e, Charset charset) {
		super(e);
		setPacketNumber(0);	// packet number must be 0 for this message
		this.charset = charset;
	}

	public MyHandshakeErrorResponse(Charset charset) {
		super();
		this.charset = charset;
	}

	public MyHandshakeErrorResponse(Exception e) {
		super(e);
		//TOCHARSET
		this.charset = Charset.defaultCharset();
	}

	@Override
	public void marshallMessage(ByteBuf cb) {
		cb.writeByte(ERRORPKT_FIELD_COUNT);
		cb.writeShort((short) getErrorNumber());
		cb.writeBytes(getErrorMsg().getBytes(charset));
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.SERVER_GREETING_ERROR_RESPONSE;
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		cb.readByte();
		setErrorNumber(cb.readShort());
		setErrorMsg(cb.slice().toString(charset));
	}

	public PEException asException() {
		return new PEException("Exception on server connect (" + getErrorNumber() + ": " + getErrorMsg() + ")");
	}
}

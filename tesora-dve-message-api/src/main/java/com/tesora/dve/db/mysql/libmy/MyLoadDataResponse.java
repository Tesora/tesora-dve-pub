// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

public class MyLoadDataResponse extends MyResponseMessage {

	String fileName;
	
	public MyLoadDataResponse(String fileName) {
		this.fileName = fileName;
	}
	
	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeByte(0xFB);
		cb.writeBytes(fileName.getBytes(CharsetUtil.UTF_8));
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		throw new PECodingException(getClass().getSimpleName());
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.LOCAL_INFILE_DATA;
	}

}

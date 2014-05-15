// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import io.netty.buffer.ByteBuf;

import com.tesora.dve.exceptions.PEException;

public interface MyMarshallMessage {

	public abstract void marshallMessage(ByteBuf cb) throws PEException;

	public abstract boolean isMessageTypeEncoded();
}
// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import io.netty.buffer.ByteBuf;
import com.tesora.dve.exceptions.PEException;

public interface MyUnmarshallMessage {

	public abstract void unmarshallMessage(ByteBuf cb) throws PEException;

}
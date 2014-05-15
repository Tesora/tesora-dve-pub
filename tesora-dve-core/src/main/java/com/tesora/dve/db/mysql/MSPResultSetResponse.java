// OS_STATUS: public
package com.tesora.dve.db.mysql;

import io.netty.buffer.ByteBuf;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.db.mysql.libmy.MyMessageType;
import com.tesora.dve.db.mysql.libmy.MyResponseMessage;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

public class MSPResultSetResponse extends MyResponseMessage {
	
	long fieldCount;

	public MSPResultSetResponse(long fieldCount) {
		super();
		this.fieldCount = fieldCount;
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		MysqlAPIUtils.putLengthCodedLong(cb, fieldCount);
	}

	@Override
	public MyMessageType getMessageType() {
		throw new PECodingException(getClass().getSimpleName());
	}

	public long getFieldCount() {
		return fieldCount;
	}

	@Override
	public void unmarshallMessage(ByteBuf cb)
			throws PEException {
	}

}

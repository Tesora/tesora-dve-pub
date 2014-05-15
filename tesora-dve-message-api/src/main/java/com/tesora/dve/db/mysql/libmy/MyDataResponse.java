// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import io.netty.buffer.ByteBuf;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

public abstract class MyDataResponse extends MyResponseMessage {
    ColumnSet columnSet;
	ResultRow row;

	public MyDataResponse(ColumnSet columnSet, ResultRow row) {
		super();
		this.columnSet = columnSet;
		this.row = row;
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) throws PEException {
		throw new PECodingException(getClass().getSimpleName());
	}

	@Override
	public MyMessageType getMessageType() {
		throw new PECodingException(getClass().getSimpleName());
	}

	public ResultRow getRow() {
		return row;
	}

}

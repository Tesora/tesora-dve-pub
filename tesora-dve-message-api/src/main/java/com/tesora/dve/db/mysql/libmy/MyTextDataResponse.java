// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import com.tesora.dve.db.NativeResultHandler;
import io.netty.buffer.ByteBuf;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

public class MyTextDataResponse extends MyDataResponse {
    NativeResultHandler nativeResultHandler;

    public MyTextDataResponse(NativeResultHandler nativeResultHandler, ColumnSet columnSet, ResultRow row) {
		super(columnSet, row);
        this.nativeResultHandler = nativeResultHandler;
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		for (int col = 1; col <= columnSet.size(); ++col) {
			MysqlAPIUtils.putLengthCodedString(	cb,
                    nativeResultHandler.getObjectAsBytes(
							columnSet.getColumn(col),
							row.getResultColumn(col).getColumnValue()), false);
		}
	}
}

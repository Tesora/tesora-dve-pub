// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import com.tesora.dve.db.NativeResultHandler;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultColumn;

public class MyRowDataResponse extends MyResponseMessage {
    NativeResultHandler dbNativeResultHandler;
	private List<ResultColumn> columnValues = new ArrayList<ResultColumn>();
	private ColumnSet columnSet;

	public MyRowDataResponse(NativeResultHandler dbNativeResultHandler, ColumnSet columnSet) {
        this.dbNativeResultHandler = dbNativeResultHandler;
		this.columnSet = columnSet;
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		ListIterator<ResultColumn> cv = columnValues.listIterator();
		int colIdx = 1;
		while (cv.hasNext()) {
			Object colVal = cv.next().getColumnValue();
            MysqlAPIUtils.putLengthCodedString(	cb,
					dbNativeResultHandler.getObjectAsBytes(columnSet.getColumn(colIdx++),
                            colVal), false);
		}
	}

	public Object getColumnValue(int colIdx) {
		return columnValues.get(colIdx).getColumnValue();
	}

	public void addColumnValue(Object columnValue) {
		this.columnValues.add(new ResultColumn(columnValue));
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.ROWDATA_RESPONSE;
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		throw new PECodingException("Method not supported for " + this.getClass().getName() );
	}

}

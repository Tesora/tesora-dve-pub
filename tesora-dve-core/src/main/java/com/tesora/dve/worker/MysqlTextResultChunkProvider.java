package com.tesora.dve.worker;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.tesora.dve.db.mysql.libmy.MyFieldPktResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyTextResultRow;

import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.db.ResultChunkProvider;
import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;

public class MysqlTextResultChunkProvider extends MysqlTextResultCollector implements ResultChunkProvider {

	ResultChunk chunk = new ResultChunk();

	public MysqlTextResultChunkProvider() {
		super(true); // need MysqlTextResultCollector with needsMetadata turned on
	}

	@Override
	public void consumeField(int field2, MyFieldPktResponse columnDef, ColumnInfo columnInfo)  {
		if (getRowDataList().size() > 0)
			throw new PECodingException("Attempt to reuse " + this.getClass().getSimpleName() + " that contains results");
		super.consumeField(field2, columnDef, columnInfo);
	}


    public void consumeRowText(MyTextResultRow textRow) {
        if  (getRowDataList().size() >= getResultsLimit())
            return;

        ArrayList<String> row = new ArrayList<String>(getFieldCount());
        ResultRow resultRow = new ResultRow();
        String colValue;
        Object colValueObj;
        for (int i = 0; i < getFieldCount(); ++i) {
            ColumnMetadata cMd = columnSet.getColumn(i + 1);
            if ( cMd.isBinaryType() ) {
                colValueObj = textRow.getBytes(i);
                colValue = textRow.getString(i, CharsetUtil.ISO_8859_1);
            } else {
                colValue = textRow.getString(i, CharsetUtil.UTF_8);
                try {
                    colValueObj = colValue == null ? null : DBTypeBasedUtils.getMysqlTypeFunc(
                            MyFieldType.fromByte((byte) cMd.getNativeTypeId()), cMd.getSize(), cMd.getNativeTypeFlags())
                                        .convertStringToObject(colValue, cMd);
                } catch (PEException e) {
                    throw new PECodingException("Problem finding value conversion function", e);
                }
            }
            resultRow.addResultColumn(new ResultColumn(colValueObj));
            row.add(i, colValue);
        }
        getRowDataList().add(row);
        chunk.addResultRow(resultRow);
    }

    @Override
	public void consumeFieldEOF(MyMessage unknown) {
		super.consumeFieldEOF(unknown);
		chunk.setColumnSet(columnSet);
	}

	@Override
	public void inject(ColumnSet metadata, List<ResultRow> rows)
			throws PEException {
		super.inject(metadata, rows);
		chunk.setColumnSet(metadata);
		chunk.setRowList(rows);
	}

	@Override
	public ColumnSet getColumnSet() {
		return chunk.getColumnSet();
	}

	public Object getSingleColumnValue(int rowIndex, int columnIndex) throws PEException {
		return chunk.getSingleValue(rowIndex, columnIndex).getColumnValue();
	}
	
	public ResultChunk getResultChunk() {
		return chunk;
	}
}

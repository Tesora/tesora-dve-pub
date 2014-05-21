package com.tesora.dve.db.mysql.libmy;

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

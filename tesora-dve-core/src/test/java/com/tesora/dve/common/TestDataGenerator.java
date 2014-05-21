// OS_STATUS: public
package com.tesora.dve.common;

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

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;

public class TestDataGenerator {
	ColumnSet columns;
	Random generator;
	List<ResultRow> generatedRows;
	Map<String, Integer> columnMap;
	String baseString;

	public TestDataGenerator(ColumnSet columns) {
		this.columns = columns;
		generatedRows = new ArrayList<ResultRow>();
		generator = new Random(1); // we don't actually want the data to be random
		baseString = RandomStringUtils.randomAlphabetic(1000);
	}

	public List<Object> generateRow(int nullCount) {
		ResultRow rr = new ResultRow();
		int nullStartIndex = columns.size() - nullCount;
		int colIndex = 1;
		for (ColumnMetadata cm : columns.getColumnList()) {
			if (colIndex >= nullStartIndex) {
				rr.addResultColumn(null);
			} else {
				rr.addResultColumn(getColumnValue(cm));
			}
		}

		generatedRows.add(rr);
		
		return getGeneratedRow(generatedRows.size()-1);
	}

	public List<Object> getGeneratedRow(int index) {
		List<Object> rv = new ArrayList<Object>();
		for (ResultColumn rc : generatedRows.get(index).getRow()) {
			rv.add(rc.getColumnValue());
		}
		
		return rv;
	}
	
	public Object getColumnValue(int rowIndex, String colName) {
		if ( columnMap == null )
			columnMap = columns.getColumnMap(false);
		
		return generatedRows.get(rowIndex).getResultColumn(columnMap.get(colName)+1).getColumnValue();
	}

	protected Object getColumnValue(ColumnMetadata cm) {
		Object cv = null;
		Calendar cal = Calendar.getInstance();

		switch (cm.getDataType()) {
		case Types.BIT:
		case Types.BOOLEAN:
			cv = Boolean.TRUE;
			break;
		case Types.BIGINT:
			cv = Long.MAX_VALUE;
			break;
		case Types.CHAR:
		case Types.VARCHAR:
			cv = StringUtils.left(baseString, cm.getSize());
			break;
		case Types.SMALLINT:
			cv = Short.MAX_VALUE;
			break;
		case Types.TINYINT:
			cv = Byte.MAX_VALUE;
			break;
		case Types.INTEGER:
			cv = Integer.MAX_VALUE;
			break;
		case Types.DOUBLE:
			cv = new Double(1234.5678);	// TODO need to handle s,p
			break;
		case Types.FLOAT:
			cv = new Float(123.56);	// TODO need to handle s,p
			break;
		case Types.DECIMAL:
			cv = new BigDecimal("12345.6789");	// TODO need to handle s,p
			break;
		case Types.DATE:
			cal.setTimeInMillis(123456789);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			cv = cal.getTime();
			break;
		case Types.TIMESTAMP:
			cal.setTimeInMillis(123456789);
			cv = cal.getTime();
			break;
		case Types.TIME:
			cv = new Time(123456789);
			break;
		default:
			break;
		}

		return cv;
	}
}
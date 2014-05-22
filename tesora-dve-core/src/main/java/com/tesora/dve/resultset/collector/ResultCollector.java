package com.tesora.dve.resultset.collector;

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


import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;

public interface ResultCollector {

	public void close() throws PEException;
	
	// getRowCount is a test method that consumes all the results of a query
	public long getRowCount() throws PEException;

	public long getUpdateCount() throws PEException;
	
	public boolean hasResults() throws PEException;

	public ColumnSet getMetadata();

	public boolean noMoreData() throws PEException;

	public void printRows() throws PEException;
	
	public ResultChunk getChunk();

	public static class ResultCollectorFactory {
		
		@SuppressWarnings("serial")
		private static final Map<Integer, ColumnSet> TYPE_MAP = 
				Collections.unmodifiableMap(new HashMap<Integer, ColumnSet>(){{
					put(Types.VARCHAR, new ColumnSet().addColumn("Value", 255, "varchar", Types.VARCHAR));
					put(Types.INTEGER, new ColumnSet().addColumn("Value", 4, "int", Types.INTEGER));
					put(Types.BOOLEAN, new ColumnSet().addColumn("Value", 1, "boolean", Types.BOOLEAN));
				}});
		
		public static ResultCollector getInstance(ColumnSet cs, ResultChunk rChunk ) {
			return new ResultCollectorStatic(cs, rChunk);
		}
		
		public static ResultCollector getInstance(int type, Object value) {
			if (!TYPE_MAP.containsKey(type))
				throw new PECodingException("ResultCollector doesn't support type " + type);
			ResultColumn rcol = new ResultColumn(value);
			ResultRow row = new ResultRow(Arrays.asList(rcol));
			ResultChunk rc = new ResultChunk(Arrays.asList(row));
			return getInstance(TYPE_MAP.get(type), rc);
		}
		
		public static ResultCollector getInstance() throws PEException {
			return new ResultCollectorStatic();
		}
		
		public static ResultCollector getInstance(int rowCount, int updateCount) {
			return new ResultCollectorStatic(rowCount, updateCount);
		}

	}
}

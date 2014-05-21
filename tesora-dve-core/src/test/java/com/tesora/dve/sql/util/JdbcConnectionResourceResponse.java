package com.tesora.dve.sql.util;

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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

public class JdbcConnectionResourceResponse extends ResourceResponse {

	private ResultSet results;
	private Long updateCount;
	private long lastInsertId; 
	
	public JdbcConnectionResourceResponse(ResultSet res, Long uc, long lastInsertId) {
		results = res;
		updateCount = uc;
		this.lastInsertId = lastInsertId;
	}
	
	@Override
	public long getNumRowsAffected() {
		return (this.updateCount != null) ? this.updateCount : 0;
	}

	@Override
	public void assertEqualResponse(String cntxt, ResourceResponse other)
			throws Throwable {
		if (results != null) {
			// result set, not much we can do
		} else if (other instanceof JdbcConnectionResourceResponse) {
			JdbcConnectionResourceResponse othResp = (JdbcConnectionResourceResponse) other;
			assertEquals(cntxt + " row count",updateCount, othResp.updateCount);			
		} else if (other instanceof ProxyConnectionResourceResponse) {
			ProxyConnectionResourceResponse othResp = (ProxyConnectionResourceResponse) other;
			assertEquals(cntxt + " row count",updateCount, new Long(othResp.getNumRowsAffected()));
		} else if (other == null) {
			// if the other side is null - that's a problem
			fail("missing other response");
		} else {
			throw new Throwable("What kind of ResourceResponse is " + other.getClass().getName());
		}
	}

	@Override
	public List<ResultRow> getResults() throws Throwable {
		return convertNativeResults(results);
	}

	@Override
	public List<ColumnChecker> getColumnCheckers() throws Throwable {
		ResultSetMetaData rsmd = results.getMetaData();
		ArrayList<ColumnChecker> checkers = new ArrayList<ColumnChecker>();
		for(int i = 1; i <= rsmd.getColumnCount(); i++) {
			int dt = rsmd.getColumnType(i);
			if (dt == Types.BINARY || dt == Types.VARBINARY || dt == Types.LONGVARBINARY)
				checkers.add(BLOB_COLUMN);
			else if (dt == Types.TIMESTAMP)
				checkers.add(TIMESTAMP_COLUMN);
			else
				checkers.add(REGULAR_COLUMN);
		}
		return checkers;
	}

	@Override
	public long getLastInsertId() throws PEException {
		return lastInsertId;
	}
	
	private void assertEqualJdbcMetadata(String cntxt, JdbcConnectionResourceResponse other) throws Throwable {
		ResultSetMetaData expected = results.getMetaData();
		ResultSetMetaData actual = other.results.getMetaData();
		assertEquals(cntxt + " result set width", expected.getColumnCount(), actual.getColumnCount());
		int nCols = expected.getColumnCount();
		for(int i = 1; i <= nCols; i++) {
			for(int f = 0; f < mdfields.length; f++) {
				mdfields[f].assertSame(cntxt, i, expected, actual);
			}
		}		
	}

	private void assertEqualProxyConnMetadata(String cntxt, ProxyConnectionResourceResponse pcrr) throws Throwable {
		ResultSetMetaData rsmd = results.getMetaData();
		ColumnSet sysColumns = pcrr.getColumns();
		assertEquals(cntxt + " mismatched column set width", rsmd.getColumnCount(), sysColumns.getColumnList().size());
		List<ColumnMetadata> sysCols = sysColumns.getColumnList();
		for(int i = 0; i < rsmd.getColumnCount(); i++) {
			ColumnMetadata sc = sysCols.get(i);
			String colcntxt = cntxt + " column " + sc.getAliasName();
			// still don't handle non column labels right
			assertEquals(colcntxt + " mismatched column name",rsmd.getColumnName(i + 1),sc.getName());
			assertEquals(colcntxt + " mismatched column label", rsmd.getColumnLabel(i + 1),sc.getAliasName());
			if (rsmd.getColumnType(i + 1) != sc.getDataType()) {
				// emit names - easier to read
				fail(colcntxt + " mismatched column type.  Expected " + rsmd.getColumnTypeName(i + 1) + " (" + rsmd.getColumnType(i + 1) + ") but found "
						+ sc.getNativeTypeName() + " (" + sc.getDataType() + ")");
			}
		}		
	}
	
	@Override
	public void assertEqualMetadata(String cntxt, ResourceResponse other)
			throws Throwable {
		if (other instanceof JdbcConnectionResourceResponse) {
			// handle differently
			assertEqualJdbcMetadata(cntxt, (JdbcConnectionResourceResponse)other);
		} else {
			assertEqualProxyConnMetadata(cntxt, (ProxyConnectionResourceResponse)other);
		}
	}
	
	private List<ResultRow> convertNativeResults(ResultSet mrs) throws Throwable {
		ResultSetMetaData rsmd = mrs.getMetaData();
		ArrayList<ResultRow> converted = new ArrayList<ResultRow>();
		while(mrs.next()) {
			ResultRow rr = new ResultRow();
			for(int i = 0; i < rsmd.getColumnCount(); i++) {
				rr.addResultColumn(mrs.getObject(i + 1), mrs.wasNull());
			}
			converted.add(rr);
		}
		return converted;
	}

	private static abstract class CheckMetadata {
		
		private String tag;
		
		public CheckMetadata(String what) {
			tag = what;
		}
		
		public void assertSame(String cntxt, int i, ResultSetMetaData expected, ResultSetMetaData actual) throws Exception {
			assertEquals(cntxt + ", column " + i + " " + tag,
					getValue(expected,i), getValue(actual,i));
		}
		
		public abstract Object getValue(ResultSetMetaData rsmd, int column) throws Exception;
	}
	
	private static CheckMetadata[] mdfields = new CheckMetadata[] {
		// catalog name won't work because the two connections use different dbs
		//		new CheckMetadata("catalog name") { public Object getValue(ResultSetMetaData rsmd, int column) throws Exception { return rsmd.getCatalogName(column); }},
		// why doesn't column class name work?
		// new CheckMetadata("column class name") { public Object getValue(ResultSetMetaData rsmd, int column) throws Exception { return rsmd.getColumnClassName(column); }},
		// In some cases we end up with a different length than native - for example when we redist select count(*) we end up with 43 instead of 21
		// 	- because we convert count to sum
		// new CheckMetadata("column display size") { public Object getValue(ResultSetMetaData rsmd, int column) throws Exception { return rsmd.getColumnDisplaySize(column); }},
		new CheckMetadata("column label") { @Override
		public Object getValue(ResultSetMetaData rsmd, int column) throws Exception { return rsmd.getColumnLabel(column); }},
		new CheckMetadata("column name") { @Override
		public Object getValue(ResultSetMetaData rsmd, int column) throws Exception { return rsmd.getColumnName(column); }},
		new CheckMetadata("column type") { @Override
		public Object getValue(ResultSetMetaData rsmd, int column) throws Exception { return rsmd.getColumnType(column); }},
		// new CheckMetadata("column type name") { public Object getValue(ResultSetMetaData rsmd, int column) throws Exception { return rsmd.getColumnTypeName(column); }},
		//new CheckMetadata("precision") { public Object getValue(ResultSetMetaData rsmd, int column) throws Exception { return rsmd.getPrecision(column); }},
		//new CheckMetadata("scale") { public Object getValue(ResultSetMetaData rsmd, int column) throws Exception { return rsmd.getScale(column); }},
	};
	
}

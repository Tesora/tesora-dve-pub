// OS_STATUS: public
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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.db.ResultChunkProvider;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

public class ProxyConnectionResourceResponse extends ResourceResponse {

	ResultChunkProvider results;
	
	public ProxyConnectionResourceResponse(ResultChunkProvider results) {
		super();
		this.results = results;
	}
	
	public ColumnSet getColumns() {
		return results.getColumnSet();
	}
	
	public boolean hasResults() {
		return results.hasResults();
	}
	
	@Override
	public long getNumRowsAffected() {
		return results.getNumRowsAffected();
	}
	
	private ProxyConnectionResourceResponse asProxyResponse(ResourceResponse other) throws Throwable {
		if (other instanceof ProxyConnectionResourceResponse)
			return (ProxyConnectionResourceResponse) other;
		throw new Throwable("Can't compare ProxyConnectionResourceResponse to " + other.getClass().getName());
	}

	@Override
	public long getLastInsertId() throws PEException {
		return results.getLastInsertId();
	}

	@Override
	public void assertEqualResponse(String message, ResourceResponse other) throws Throwable {
		ProxyConnectionResourceResponse yours = asProxyResponse(other);
		assertEquals(message + " mismatched lastInsertID", getLastInsertId(), yours.getLastInsertId());
		assertEquals(message + " mismatched hasResults", hasResults(), yours.hasResults());
	}

	@Override
	public List<ResultRow> getResults() throws Throwable {
		return results.getResultChunk().getRowList();
	}

	@Override
	public List<ColumnChecker> getColumnCheckers() throws Throwable {
		ColumnSet rsmd = getColumns();
		if (rsmd == null)
			return Collections.emptyList();
		List<ColumnMetadata> cols = rsmd.getColumnList();
		List<ColumnChecker> checkers = new ArrayList<ColumnChecker>();
		for(ColumnMetadata cc : cols) {
			if (cc.getDataType() == Types.VARBINARY || cc.getDataType() == Types.LONGVARBINARY ||
					cc.getDataType() == Types.BINARY)
				checkers.add(BLOB_COLUMN);
			else
				checkers.add(REGULAR_COLUMN);
		}
		return checkers;
	}

	@Override
	public void assertEqualMetadata(String cntxt, ResourceResponse other)
			throws Throwable {
		ProxyConnectionResourceResponse yours = asProxyResponse(other);
		ColumnSet myColumns = getColumns();
		ColumnSet yourColumns = yours.getColumns();
		assertEquals("mismatched column set width", myColumns.getColumnList().size(), yourColumns.getColumnList().size());
		List<ColumnMetadata> checkCols = myColumns.getColumnList();
		List<ColumnMetadata> sysCols = yourColumns.getColumnList();
		for(int i = 0; i < checkCols.size(); i++) {
			ColumnMetadata cc = checkCols.get(i);
			ColumnMetadata sc = sysCols.get(i);
			assertEquals("mismatched column name",cc.getAliasName(),sc.getAliasName());
			assertEquals("mismatched column type",cc.getDataType(),sc.getDataType());
		}
	}
}

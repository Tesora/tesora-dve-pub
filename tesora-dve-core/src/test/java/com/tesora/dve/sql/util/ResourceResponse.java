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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.SchemaTest;

public abstract class ResourceResponse {
	
	public abstract void assertEqualResponse(String cntxt, ResourceResponse other) throws Throwable;
	
	public abstract List<ResultRow> getResults() throws Throwable;
	public abstract List<ColumnChecker> getColumnCheckers() throws Throwable;
	
	public abstract void assertEqualMetadata(String cntxt, ResourceResponse other) throws Throwable;

	public abstract long getLastInsertId() throws PEException;
	
	public abstract long getNumRowsAffected();

	public void assertEqualResults(String cntxt, boolean ignoreOrder, boolean ignoreMD, ResourceResponse other) throws Throwable {
		if (!ignoreMD) assertEqualMetadata(cntxt, other);
		List<ResultRow> myResults = getResults();
		List<ResultRow> yourResults = other.getResults();
		List<ColumnChecker> colchecks = getColumnCheckers();
		if (ignoreOrder)
			assertResultsEqualUnordered(cntxt, myResults, yourResults, colchecks);
		else
			assertResultsEqual(cntxt, myResults, yourResults, colchecks);
	}
	
	public void assertResults(String cntxt, Object[] values) throws Throwable {
		List<ResultRow> results = getResults();
		if (results.size() != values.length) {			
			System.out.println("wtf?");
			System.out.println(displayRows(cntxt,false));
		}
		assertEquals(cntxt + " result size mismatch", values.length, results.size());
		List<ColumnChecker> checkers = getColumnCheckers();
		for(int i = 0; i < values.length; i++) 
			assertRowEquals(cntxt + ":" + results.get(i), results.get(i), (Object[])values[i], checkers);
	}
	
	public static final ColumnChecker REGULAR_COLUMN = new ColumnChecker();
	public static final BlobColumnChecker BLOB_COLUMN = new BlobColumnChecker();
	public static final TimestampColumnChecker TIMESTAMP_COLUMN = new TimestampColumnChecker();

	public String displayRows(String header, boolean showTypes) throws Throwable {
		return displayRows(header, getResults(), getColumnCheckers(), showTypes);
	}
	
	private String displayRows(String header, List<ResultRow> results, List<ColumnChecker> checkers, boolean showTypes) throws Throwable {
		StringBuffer buf = new StringBuffer();
		if (header != null)
			buf.append(header).append(PEConstants.LINE_SEPARATOR);
		for(int i = 0; i < results.size(); i++)
			buf.append(displayRow(null,results.get(i), checkers, i, showTypes)).append(PEConstants.LINE_SEPARATOR);	
		return buf.toString();
	}
	
	private String displayRow(String header, ResultRow rr, List<ColumnChecker> checkers, int offset, boolean types) throws Throwable {
		List<ResultColumn> f = rr.getRow();
		StringBuilder sb = new StringBuilder();
		if (header != null)
			sb.append(header).append(PEConstants.LINE_SEPARATOR);
		sb.append("[").append(offset).append("]: ");
		for(int i = 0; i < f.size(); i++) {
			ResultColumn rc = f.get(i);
			ColumnChecker cc = (checkers.isEmpty() ? REGULAR_COLUMN : checkers.get(i));
			if (rc.isNull())
				sb.append(" '(null value)'");
			else
				sb.append(" '").append(cc.asString(rc.getColumnValue())).append("'");
			if (types && rc.getColumnValue() != null)
				sb.append(" (").append(rc.getColumnValue().getClass().getName()).append(")");
		}
		return sb.toString();
	}

	private String buildRowsMessage(String cntxt, List<ResultRow> left, List<ResultRow> right, List<ColumnChecker> checkers) throws Throwable {
		StringBuilder fullMessage = new StringBuilder();
		if (cntxt != null)
			fullMessage.append(cntxt).append(PEConstants.LINE_SEPARATOR);
		fullMessage.append(displayRows("Left rows",left,checkers,true));
		fullMessage.append(displayRows("Right rows",right,checkers,true));
		return fullMessage.toString();
	}
	
	public void assertResultsEqual(String cntxt, List<ResultRow> left, List<ResultRow> right, List<ColumnChecker> checkers) throws Throwable {
		String acntxt = cntxt;
		if (acntxt == null)
			acntxt = "";
		if (left.size() != right.size()) 
			assertEquals(buildRowsMessage(acntxt + " result set size",left,right,checkers), left.size(), right.size());
		Iterator<ResultRow> liter = left.iterator();
		Iterator<ResultRow> riter = right.iterator();
		int offset = 0;
		while(liter.hasNext() && riter.hasNext()) {
			ResultRow lrow = liter.next();
			ResultRow rrow = riter.next();
			String message = assertRowsEqual(acntxt, lrow.getRow(), rrow.getRow(), checkers);
			if (message != null) 
				// in this case, we're still going to emit the full result sets, but also emit the row that differs
				fail(buildRowsMessage("row " + offset + ": " + message,left,right,checkers));
		}
	}
	
	public void assertResultsEqualUnordered(String cntxt, List<ResultRow> left, List<ResultRow> right, List<ColumnChecker> checkers) throws Throwable {
		String acntxt = cntxt;
		if (acntxt == null)
			acntxt = "";
		if (left.size() != right.size()) 
			assertEquals(buildRowsMessage(acntxt + " result set size",left,right,checkers), left.size(), right.size());
		MultiMap<String, List<ResultColumn>> leftRows = buildUnorderedMap(left, checkers);
		MultiMap<String, List<ResultColumn>> rightRows = buildUnorderedMap(right, checkers);
		pruneMatching(acntxt,leftRows, rightRows, checkers);
		pruneMatching(acntxt,rightRows, leftRows, checkers);
		if (!leftRows.isEmpty() || !rightRows.isEmpty()) 
			assertTrue(buildRowsMessage(acntxt + " differing result rows", left, right, checkers), leftRows.isEmpty() && rightRows.isEmpty());
	}
	
	private void pruneMatching(String cntxt, MultiMap<String, List<ResultColumn>> lhs, MultiMap<String, List<ResultColumn>> rhs, List<ColumnChecker> checkers) {
		MultiMap<String, List<ResultColumn>> leftRemoved = new MultiMap<String, List<ResultColumn>>();
		MultiMap<String, List<ResultColumn>> rightRemoved = new MultiMap<String, List<ResultColumn>>();
		for(String k : lhs.keySet()) {
			Collection<List<ResultColumn>> lsub = lhs.get(k);
			Collection<List<ResultColumn>> rsub = rhs.get(k);
			if (rsub == null || rsub.isEmpty())
				continue;
			for(Iterator<List<ResultColumn>> liter = lsub.iterator(); liter.hasNext();) {
				List<ResultColumn> lr = liter.next();
				for(Iterator<List<ResultColumn>> riter = rsub.iterator(); riter.hasNext();) {
					List<ResultColumn> rr = riter.next();
					if (assertRowsEqual(cntxt, lr, rr, checkers) == null) {
						leftRemoved.put(k,lr);
						rightRemoved.put(k, rr);
					}
				}
			}
		}
		for(String k : leftRemoved.keySet()) {
			for(List<ResultColumn> rr : leftRemoved.get(k))
				lhs.remove(k, rr);
		}
		for(String k : rightRemoved.keySet()) {
			for(List<ResultColumn> rr : rightRemoved.get(k))
				rhs.remove(k, rr);
		}
	}
	
	private MultiMap<String, List<ResultColumn>> buildUnorderedMap(List<ResultRow> in, List<ColumnChecker> checkers) {
		MultiMap<String, List<ResultColumn>> ret = new MultiMap<String, List<ResultColumn>>();
		for(ResultRow rr : in) {
			ret.put(computeHashKey(rr.getRow(), checkers), rr.getRow());
		}
		return ret;
	}
	
	private String computeHashKey(List<ResultColumn> rr, List<ColumnChecker> checkers) {
		StringBuffer buf = new StringBuffer();
		for(int i = 0; i < checkers.size(); i++) {
			ColumnChecker cc = checkers.get(i);
			buf.append(cc.asString(rr.get(i).getColumnValue())).append(";");
		}
		return buf.toString();
	}
	
	private String assertRowsEqual(String cntxt, List<ResultColumn> lrow, List<ResultColumn> rrow, List<ColumnChecker> checkers) {
		if ((lrow.size() != rrow.size()))
			return cntxt + " sizes differ: expected " + lrow.size() + " but got " + rrow.size();
		if (lrow.size() != checkers.size()) 
			return cntxt + " wrong number of column checkers.  Have " + checkers.size() + " but row has " + lrow.size() + " columns";
		for(int i = 0; i < checkers.size(); i++) {
			ColumnChecker cc = checkers.get(i);
			ResultColumn lrc = lrow.get(i);
			ResultColumn rrc = rrow.get(i);
			String diffs = cc.isEqual(cntxt, lrc.getColumnValue(), rrc.getColumnValue(), true);
			if (diffs != null)
				return diffs;
		}
		return null;
	}
		
	private void assertRowEquals(String cntxt, ResultRow rr, Object[] values, List<ColumnChecker> checkers) {
		List<ResultColumn> row = rr.getRow();
//		if (row.size() != values.length)
//			System.out.println("wtf?");
		assertEquals(cntxt + " row width",row.size(),values.length);
		assertEquals(row.size(), checkers.size());
		for(int i = 0; i < values.length; i++) {
			if (values[i] == SchemaTest.getIgnore())
				continue;
			String diffs = checkers.get(i).isEqual(cntxt, values[i], row.get(i).getColumnValue(), false);
			if (diffs != null) fail(diffs);
		}
	}
}

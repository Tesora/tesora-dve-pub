// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

public class StepExecutionStatistics {

	protected final AtomicLong elapsed;
	protected final AtomicInteger calls;
	// for update, delete - number of rows
	// for selects - rows returned
	// for redist - rows from the src select
	protected final AtomicLong rows;
	
	public StepExecutionStatistics() {
		this.elapsed = new AtomicLong(0);
		this.calls = new AtomicInteger(0);
		this.rows = new AtomicLong(0);
	}
	
	public void onExecution(long elapsed, long rows) {
		this.elapsed.addAndGet(elapsed);
		this.rows.addAndGet(rows);
		this.calls.getAndIncrement();
	}
	
	public long getElapsed() {
		return elapsed.get();
	}
	
	public int getCalls() {
		return calls.get();
	}
	
	public long getRows() {
		return rows.get();
	}
	
	protected static final String[] statsExplainColumns =
			new String[] { "Count", "Rows", "Elapsed" };

	public static void addColumnHeaders(ColumnSet cs) {
		for(String s : statsExplainColumns) 
			cs.addColumn(s,3,"integer",Types.INTEGER);
	}
	
	public void addColumns(ResultRow rr) {
		rr.addResultColumn(calls.get());
		rr.addResultColumn(rows.get());
		rr.addResultColumn(elapsed.get());
	}
}

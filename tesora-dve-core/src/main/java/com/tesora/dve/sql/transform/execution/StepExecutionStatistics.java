// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

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

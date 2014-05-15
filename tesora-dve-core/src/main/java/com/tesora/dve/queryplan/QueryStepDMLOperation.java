// OS_STATUS: public
package com.tesora.dve.queryplan;


import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.sql.transform.execution.StepExecutionStatistics;

public abstract class QueryStepDMLOperation extends QueryStepOperation {

	StepExecutionStatistics statistics;
	long before;
	
	protected final PersistentDatabase database;
	
	protected QueryStepDMLOperation(PersistentDatabase pdb) {
		this.statistics = null;
		this.database = pdb;
	}
	
	public void setStatistics(StepExecutionStatistics stats) {
		this.statistics = stats;
	}
	
	protected void beginExecution() {
		if (this.statistics == null) return;
		before = System.nanoTime();
	}
	
	protected void endExecution(long rows) {
		if (this.statistics == null) return;
		this.statistics.onExecution(System.nanoTime() - before, rows);
	}
	
	@Override
	public final PersistentDatabase getContextDatabase() {
		return database;
	}
	
}

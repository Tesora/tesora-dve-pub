package com.tesora.dve.sql.schema.cache.qstat;

public interface StepExecutionStatistics {

	public void onExecution(long elapsed, long rows);
	
}

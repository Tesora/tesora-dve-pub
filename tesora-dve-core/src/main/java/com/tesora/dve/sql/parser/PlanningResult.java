// OS_STATUS: public
package com.tesora.dve.sql.parser;

import java.util.List;

import com.tesora.dve.sql.transform.execution.ExecutionPlan;

public class PlanningResult {

	private final List<ExecutionPlan> plans;
	// when the state is null - the whole input was consumed
	private final InputState state;
	// we may need to track the input sql
	private final String originalSQL;
	
	public PlanningResult(List<ExecutionPlan> plans, InputState state, String origSQL) {
		this.plans = plans;
		this.state = state;
		this.originalSQL = origSQL;
	}
	
	public boolean hasMore() {
		return this.state != null;
	}
	
	public List<ExecutionPlan> getPlans() {
		return plans;
	}
	
	public InputState getInputState() {
		return this.state;
	}
	
	public boolean isPrepare() {
		return false;
	}
	
	public String getOriginalSQL() {
		return originalSQL;
	}
}

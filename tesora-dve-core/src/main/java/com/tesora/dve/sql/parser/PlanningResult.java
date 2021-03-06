package com.tesora.dve.sql.parser;

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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.transform.execution.ConnectionValuesMap;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;

public class PlanningResult {

	private final List<ExecutionPlan> plans;
	private final ConnectionValuesMap values;
	// when the state is null - the whole input was consumed
	private final InputState state;
	// we may need to track the input sql
	private final String originalSQL;
	
	public PlanningResult(List<ExecutionPlan> plans, ConnectionValuesMap values, InputState state, String origSQL) {
		this.plans = plans;
		this.state = state;
		this.originalSQL = origSQL;
		this.values = values;
	}
	
	public PlanningResult(ExecutionPlan singlePlan, ConnectionValuesMap values, InputState state, String origSQL) {
		this.plans = new ArrayList<ExecutionPlan>();
		this.plans.add(singlePlan);
		this.state = state;
		this.originalSQL = origSQL;
		this.values = values;
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
	
	public ConnectionValuesMap getValues() {
		return this.values;
	}
	
	public boolean isPrepare() {
		return false;
	}
	
	public String getOriginalSQL() {
		return originalSQL;
	}
}

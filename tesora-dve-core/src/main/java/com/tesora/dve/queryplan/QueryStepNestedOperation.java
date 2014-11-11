package com.tesora.dve.queryplan;

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

import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.LateBoundConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepNestedOperation extends QueryStepOperation {

	private final QueryStepOperation target;
	private final ExecutionPlan plan;
	
	public QueryStepNestedOperation(QueryStepOperation actual, ExecutionPlan plan) throws PEException {
		super(actual.sg);
		this.plan = plan;
		this.target = actual;
		this.reqs.addAll(actual.reqs);
	}

	@Override
	public void executeSelf(ExecutionState execState, WorkerGroup wg,
			DBResultConsumer resultConsumer) throws Throwable {
		target.executeSelf(execState,wg,resultConsumer);
	}

	@Override
	public void execute(ExecutionState state, DBResultConsumer resultConsumer) throws Throwable {
		// we need to rebind the values for this plan now - we take the late constants off of the state
		// and bind them into our conn value, which would allocate any autoincs
		LateBoundConstants rtc = state.getBoundConstants();
		ConnectionValues myValues = state.getValuesMap().getValues(plan);
		plan.getValueManager().resetForRuntimePlan(state.getConnection().getSchemaContext(), myValues, rtc);
		ExecutionState childState = state.pushValues(myValues);
		executeRequirements(childState);
		executeOperation(childState,resultConsumer);
	}

	
}

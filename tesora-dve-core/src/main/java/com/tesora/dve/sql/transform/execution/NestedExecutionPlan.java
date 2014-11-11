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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepNestedOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;

public class NestedExecutionPlan extends ExecutionPlan {

	public NestedExecutionPlan(ValueManager valueManager) {
		super(valueManager);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean isRoot() {
		return false;
	}

	@Override
	public void setCacheable(boolean v) {
	}

	@Override
	public boolean isCacheable() {
		return true;
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValuesMap cv, ExecutionPlan currentPlan)
			throws PEException {
		// we're going to wrap our entire plan inside a special qso that caches our (mostly constant) connection values
		List<QueryStepOperation> mySteps = new ArrayList<QueryStepOperation>();
		ParserOptions was = sc.getOptions();
		try {
			sc.setOptions(was.setNestedPlan());
			super.schedule(new ExecutionPlanOptions(),mySteps,null,sc,cv,this);
			QueryStepOperation flattened = collapseOperationList(mySteps);
			qsteps.add(new QueryStepNestedOperation(flattened,this));
		} finally {
			sc.setOptions(was);
		}
	}
	
}

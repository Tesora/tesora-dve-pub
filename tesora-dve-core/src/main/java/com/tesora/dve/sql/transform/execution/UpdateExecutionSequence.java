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

import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepUpdateSequenceOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.SchemaContext;

public class UpdateExecutionSequence extends ExecutionSequence {

	public UpdateExecutionSequence(ExecutionPlan p) {
		super(p);
	}

	@Override
	protected String sequenceName() {
		return "UPDATE SEQUENCE";
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValuesMap cvm, ExecutionPlan containing)
			throws PEException {
		if (steps.isEmpty()) return;
		ArrayList<QueryStepOperation> mine = new ArrayList<QueryStepOperation>();
		// can only be used if they all use the same storage group
		ConnectionValues cv = cvm.getValues(containing);
		StorageGroup sg = null;
		for(HasPlanning hp : steps) {
			if (hp instanceof ExecutionStep) {
				ExecutionStep es = (ExecutionStep) hp;
				if (sg == null)
					sg = es.getStorageGroup(sc,cv);
				else if (!sg.equals(es.getStorageGroup(sc,cv)))
					throw new PEException("UpdateExecutionSequence created with multiple groups");
			}
			hp.schedule(opts, mine, projection, sc, cvm,containing);
		}
		QueryStepUpdateSequenceOperation uo = new QueryStepUpdateSequenceOperation(sg);
		for(QueryStepOperation qs : mine) {
			uo.addOperation(qs);
		}
		qsteps.add(uo);
	}


	
}

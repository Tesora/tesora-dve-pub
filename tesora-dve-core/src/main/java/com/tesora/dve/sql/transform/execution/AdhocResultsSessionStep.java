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

import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepAdhocResultsOperation;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.SchemaContext;

public class AdhocResultsSessionStep extends ExecutionStep {

	protected IntermediateResultSet results;
	
	public AdhocResultsSessionStep(IntermediateResultSet irs) {
		super(null,null,ExecutionType.SESSION);
		results = irs;
	}
	
	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		buf.add("ad hoc results");
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		addStep(sc,qsteps, new QueryStepAdhocResultsOperation(results));
	}

}

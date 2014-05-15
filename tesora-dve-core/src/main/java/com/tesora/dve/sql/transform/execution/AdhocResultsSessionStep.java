// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

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

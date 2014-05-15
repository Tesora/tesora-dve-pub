// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.SchemaContext;

public class EmptyExecutionStep extends ExecutionStep {

	private long updateCount;
	private String sql;
	
	public EmptyExecutionStep(long updateCount, String command) {
		super(null, null, null);
		this.updateCount = updateCount;
		this.sql = command;
	}


	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		buf.add(sql);
	}

	@Override
	public Long getUpdateCount(SchemaContext sc) {
		return updateCount;
	}


	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
	}	
	
	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		buf.add(indent + sql);
	}

}

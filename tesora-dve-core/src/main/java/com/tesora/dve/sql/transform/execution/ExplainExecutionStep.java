package com.tesora.dve.sql.transform.execution;

import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.sql.schema.SchemaContext;

public class ExplainExecutionStep extends DDLQueryExecutionStep {

	private final ExecutionPlan target;
	
	public ExplainExecutionStep(String tag, ExecutionPlan targ, IntermediateResultSet results) {
		super(tag, results);
		this.target = targ;
	}

	@Override
	public void display(SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan containing, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc,cv,containing,buf,indent,opts);
		target.display(sc,cv,containing,buf,indent + "  ",opts);
	}

	
}

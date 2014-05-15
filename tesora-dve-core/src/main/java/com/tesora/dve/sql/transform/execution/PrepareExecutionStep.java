// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;


import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepOperationPrepareStatement;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;

public class PrepareExecutionStep extends DirectExecutionStep {

	public PrepareExecutionStep(Database<?> db, PEStorageGroup storageGroup, GenericSQLCommand command) throws PEException {
		super(db, storageGroup, ExecutionType.PREPARE, null,null, command, null);
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc) throws PEException {
		addStep(sc,qsteps,new QueryStepOperationPrepareStatement(getDatabase(),getCommand(sc),projection));
	}

	// we need our own version of getCommand so that we don't attempt to resolve the parameters
	@Override
	public SQLCommand getCommand(SchemaContext sc) {
		GenericSQLCommand gen = sql.resolve(sc,true,null);
		return new SQLCommand(gen);
	}

	@Override
	public String getSQL(SchemaContext sc, EmitOptions opts) {
		return sql.resolve(sc,true,null).getUnresolved();
	}

	@Override
	public void displaySQL(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		ArrayList<String> sub = new ArrayList<String>();
		sql.display(sc, true, "  ", sub);
		for(String s : sub) {
			buf.add(indent + "    " + s);
		}
	}

}

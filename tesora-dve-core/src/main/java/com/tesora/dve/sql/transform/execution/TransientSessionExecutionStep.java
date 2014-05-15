// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;



import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepGeneralOperation;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;

public class TransientSessionExecutionStep extends ExecutionStep {

	private String sql;
	private AdhocOperation op;
	private boolean txn;
	private boolean workers;
	
	public TransientSessionExecutionStep(Database<?> db, PEStorageGroup storageGroup, String command, boolean needsTxn, boolean needsWorkers,
			AdhocOperation op) {
		super(db,storageGroup,ExecutionType.SESSION);
		this.sql = command;
		this.op = op;
		this.txn = needsTxn;
		this.workers = needsWorkers;
	}
	
	public TransientSessionExecutionStep(String command, boolean needsTxn, boolean needsWorkers, AdhocOperation op) {
		this(null,null,command,needsTxn,needsWorkers,op);
	}

	public TransientSessionExecutionStep(String command, AdhocOperation op) {
		this(command, false, false, op);
	}
	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		addStep(sc, qsteps, new QueryStepGeneralOperation(op,txn,workers));
	}
	
	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		buf.add(sql);
	}

}

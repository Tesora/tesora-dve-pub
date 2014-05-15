// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepOperationPrepareStatement extends QueryStepOperation {

	private SQLCommand toPrepare;
	private PersistentDatabase ctxDatabase;
	@SuppressWarnings("unused")
	private ProjectionInfo projection;
	
	public QueryStepOperationPrepareStatement(PersistentDatabase pdb, SQLCommand command, ProjectionInfo projInfo) {
		this.ctxDatabase = pdb;
		this.toPrepare = command;
		this.projection = projInfo;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
//		WorkerClientPrepareStatementRequest req = 
//				new WorkerClientPrepareStatementRequest(ssCon.getTransactionalContext(), ctxDatabase, toPrepare, projection);

		WorkerExecuteRequest req = new WorkerExecuteRequest(ssCon.getTransactionalContext(), toPrepare).onDatabase(ctxDatabase);
		wg.execute(MappingSolution.AnyWorker, req, resultConsumer);
	}

	@Override
	public boolean requiresTransaction() {
		return false;
	}
	
}

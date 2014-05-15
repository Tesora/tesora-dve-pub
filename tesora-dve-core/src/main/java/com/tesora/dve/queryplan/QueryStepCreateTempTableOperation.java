// OS_STATUS: public
package com.tesora.dve.queryplan;

import java.util.Collection;
import java.util.concurrent.Future;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepCreateTempTableOperation extends QueryStepOperation {

	private UserTable theTable;
	
	public QueryStepCreateTempTableOperation(UserTable toCreate) {
		theTable = toCreate;
	}

	@Override
	public PersistentDatabase getContextDatabase() {
		return theTable.getDatabase();
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg,
			DBResultConsumer results) throws Throwable {
		String sqlCommand = theTable.getCreateTableStmt(true);
		WorkerRequest req = new WorkerExecuteRequest(
				ssCon.getNonTransactionalContext(), new SQLCommand(sqlCommand))
				.onDatabase(theTable.getDatabase()).forDDL();
		ExecutionLogger logger = ssCon.getExecutionLogger().getNewLogger(
				"CreateTempTable");
		try {
			WorkerRequest preCleanupReq = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(),
					UserTable.getDropTableStmt(theTable.getName(), true))
					.onDatabase(theTable.getDatabase()).forDDL();
			wg.execute(MappingSolution.AllWorkers, preCleanupReq,DBEmptyTextResultConsumer.INSTANCE);
			Collection<Future<Worker>> f = wg.submit(MappingSolution.AllWorkers, req,
					DBEmptyTextResultConsumer.INSTANCE);
			wg.addCleanupStep(new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), UserTable.getDropTableStmt(
					theTable.getName(), false)).onDatabase(theTable.getDatabase()));
			WorkerGroup.syncWorkers(f);
		} finally {
			logger.end();
		}
	}
}

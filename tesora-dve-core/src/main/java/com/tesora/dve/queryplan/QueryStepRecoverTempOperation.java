// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepRecoverTempOperation extends QueryStepOperation {
	
	PersistentDatabase database;
	UserTable tempTable;

	public QueryStepRecoverTempOperation(PersistentDatabase database, UserTable tempTable) {
		this.database = database;
		this.tempTable = tempTable;
	}

	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		WorkerRequest req = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), UserTable.getDropTableStmt(tempTable.getName(), false)
				).onDatabase(database);
		wg.execute(MappingSolution.AllWorkers, req, resultConsumer);
//		wg.sendToAllWorkers(ssCon, req);
//		ssCon.consumeReplies(wg.size());
//		return null;
	}

	@Override
	public boolean requiresWorkers() {
		return true;
	}

}

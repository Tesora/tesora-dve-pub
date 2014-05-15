// OS_STATUS: public
package com.tesora.dve.queryplan;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepDropTempTableOperation extends QueryStepOperation {


	static Logger logger = Logger.getLogger( QueryStepDropTempTableOperation.class );
	
	UserDatabase database;
	String tableName;
	
	public QueryStepDropTempTableOperation() {
	}
	
	public QueryStepDropTempTableOperation(UserDatabase udb, String tableName) {
		this.database = udb;
		this.tableName = tableName;
	}

	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
		WorkerRequest req = 
				new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), UserTable.getDropTableStmt(tableName,false)).
				onDatabase(database);
		wg.execute(MappingSolution.AllWorkers, req, resultConsumer);
	}

	@Override
	public boolean requiresTransaction() {
		return false;
	}

}

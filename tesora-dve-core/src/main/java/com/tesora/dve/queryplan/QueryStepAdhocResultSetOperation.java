// OS_STATUS: public
package com.tesora.dve.queryplan;


import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepAdhocResultSetOperation extends QueryStepOperation {

	private IntermediateResultSet results;
	
	public QueryStepAdhocResultSetOperation(IntermediateResultSet theResults) {
		super();
		results = theResults;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
		resultConsumer.inject(results.getMetadata(), results.getRows());
	}

	
	@Override
	public boolean requiresTransaction() {
		return false;
	}

	@Override
	public boolean requiresWorkers() {
		// could be executed before the database is set
		return false;
	}

}

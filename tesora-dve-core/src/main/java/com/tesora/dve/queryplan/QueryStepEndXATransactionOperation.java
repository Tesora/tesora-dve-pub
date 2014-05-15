// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.UserXid;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepEndXATransactionOperation extends QueryStepOperation {

	UserXid xid;
	
	public QueryStepEndXATransactionOperation(UserXid id) {
		super();
		this.xid = id;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg,
			DBResultConsumer resultConsumer) throws Throwable {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean requiresWorkers() {
		return false;
	}

}

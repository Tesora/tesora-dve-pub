// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.UserXid;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepPrepareXATransactionOperation extends QueryStepOperation {

	UserXid xaXid;
	
	public QueryStepPrepareXATransactionOperation(UserXid xid) {
		super();
		xaXid = xid;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg,
			DBResultConsumer resultConsumer) throws Throwable {
		// TODO Auto-generated method stub
		// does nothing for now
	}

	@Override
	public boolean requiresWorkers() {
		return false;
	}

}

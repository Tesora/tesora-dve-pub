// OS_STATUS: public
package com.tesora.dve.queryplan;

import javax.xml.bind.annotation.XmlType;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.UserXid;
import com.tesora.dve.worker.WorkerGroup;

/**
 * {@link QueryStepRollbackTransactionOperation} is a <b>QueryStep</b> operation which
 * rolls back a transaction.
 *
 */
@XmlType(name="QueryStepBeginTransactionOperation")
public class QueryStepRollbackTransactionOperation extends QueryStepOperation {

	@SuppressWarnings("unused")
	private UserXid xaXid;
	
	public QueryStepRollbackTransactionOperation withXAXid(UserXid id) {
		this.xaXid = id;
		return this;
	}
	
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws PEException {
		ssCon.userRollbackTransaction();
		resultConsumer.rollback();
	}

	@Override
	public boolean requiresWorkers() {
		return false;
	}
}

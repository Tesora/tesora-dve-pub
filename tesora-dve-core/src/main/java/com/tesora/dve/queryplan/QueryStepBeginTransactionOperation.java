// OS_STATUS: public
package com.tesora.dve.queryplan;

import javax.xml.bind.annotation.XmlType;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.UserXid;
import com.tesora.dve.worker.WorkerGroup;

/**
 * {@link QueryStepBeginTransactionOperation} is a <b>QueryStep</b> operation which
 * begins a transaction.
 * 
 */
@XmlType(name="QueryStepBeginTransactionOperation")
public class QueryStepBeginTransactionOperation extends QueryStepOperation {
	
	boolean withConsistentSnapshot = false;
	UserXid xaXid;
	
	public QueryStepBeginTransactionOperation(boolean withConsistentSnapshot) {
		this.withConsistentSnapshot = withConsistentSnapshot;
	}

	public QueryStepBeginTransactionOperation() {
	}
	
	public QueryStepBeginTransactionOperation withConsistentSnapshot() {
		this.withConsistentSnapshot = true;
		return this;
	}

	public QueryStepBeginTransactionOperation withXAXid(UserXid id) {
		this.xaXid = id;
		return this;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws PEException {
		ssCon.userBeginTransaction(withConsistentSnapshot);
	}

	@Override
	public boolean requiresImplicitCommit() {
		return true;
	}
	
	@Override
	public boolean requiresWorkers() {
		return false;
	}
}

// OS_STATUS: public
package com.tesora.dve.queryplan;

import javax.xml.bind.annotation.XmlType;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.UserXid;
import com.tesora.dve.worker.WorkerGroup;

/**
 * {@link QueryStepCommitTransactionOperation} is a <b>QueryStep</b> operation which
 * commits a transaction.
 *
 */
@XmlType(name="QueryStepBeginTransactionOperation")
public class QueryStepCommitTransactionOperation extends QueryStepOperation {
	
	@SuppressWarnings("unused")
	private UserXid xaXid;
	@SuppressWarnings("unused")
	private boolean onePhase;

	public QueryStepCommitTransactionOperation withXAXid(UserXid xid, boolean onePhase) {
		this.xaXid = xid;
		this.onePhase = onePhase;
		return this;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		try {
			ssCon.userCommitTransaction();
		} catch (Throwable t) {
			throw new PEException("Unable to commit transaction",t);
		}
	}
	
	@Override
	public boolean requiresWorkers() {
		return false;
	}
}

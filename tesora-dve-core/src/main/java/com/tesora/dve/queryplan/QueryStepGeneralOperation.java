// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepGeneralOperation extends QueryStepOperation {

	private AdhocOperation target;
	private boolean requiresWorkers;
	private boolean requiresTxn;
		
	public QueryStepGeneralOperation(AdhocOperation adhoc, boolean requiresTxn, boolean requiresWorkers) {
		super();
		target = adhoc;
		this.requiresWorkers = requiresWorkers;
		this.requiresTxn = requiresTxn;
	}
	
	public QueryStepGeneralOperation(AdhocOperation adhoc) {
		this(adhoc, false, false);
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		target.execute(ssCon, wg, resultConsumer);
	}

	@Override
	public boolean requiresTransaction() {
		return requiresTxn;
	}
	
	/*
	 * Does this query step require workers?  Default is true.  Some DDL operations are catalog-only, and
	 * thus do not require workers.
	 */
	@Override
	public boolean requiresWorkers() {
		return requiresWorkers;
	}

	
	public interface AdhocOperation {
		
		public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable;

	}

	
}

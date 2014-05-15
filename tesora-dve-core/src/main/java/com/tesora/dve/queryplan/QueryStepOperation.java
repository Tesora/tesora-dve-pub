// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.logutil.LogSubject;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.WorkerGroup;

/**
 * Baseclass of <b>QueryStep</b> operations, which include, but are not limited to 
 * {@link QueryStepInsertByKeyOperation},
 * {@link QueryStepSelectAllOperation} and {@link QueryStepRedistOperation}.
 * @author mjones
 *
 */
public abstract class QueryStepOperation implements LogSubject {

	/**
	 * Called by <b>QueryStep</b> to execute the operation.  Any results
	 * of the operation will be returned in an instance of {@link ResultCollector}.
	 * 
	 * @param ssCon user's connection
	 * @param wg provisioned {@link WorkerGroup} against which to execute the step
	 * @param resultConsumer 
	 * @return {@link ResultCollector} with results, or <b>null</b>
	 * @throws JMSException
	 * @throws PEException
	 * @throws Throwable 
	 */
	public abstract void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable;
	
	public boolean requiresTransaction() {
		return true;
	}
	
	/**
	 * Does this query step require workers?  Default is true.  Some DDL operations are catalog-only, and
	 * thus do not require workers.
	 */
	public boolean requiresWorkers() {
		return true;
	}
	
	/**
	 * Returns true if the QueryStepOperation causes any inflight transactions to be committed
	 * first.
	 */
	public boolean requiresImplicitCommit() {
		return false;
	}	

	@Override
	public String describeForLog() {
		return this.getClass().getSimpleName();
	}
	
	public PersistentDatabase getContextDatabase() {
		return null;
	}
	
}
// OS_STATUS: public
package com.tesora.dve.queryplan;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.WorkerCreateDatabaseRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepCreateDatabaseOperation extends QueryStepOperation {

	static Logger logger = Logger.getLogger( QueryStepCreateDatabaseOperation.class );
	
	UserDatabase newDatabase;
	CacheInvalidationRecord invalidate;
	
	public QueryStepCreateDatabaseOperation(UserDatabase db, CacheInvalidationRecord cir) {
		this.newDatabase = db;
		invalidate = cir;
	}
		
	/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		if (ssCon.hasActiveTransaction())
			throw new PEException("Cannot execute DDL within active transaction: CREATE DATABASE " + newDatabase.getName());
		
		CatalogDAO c = ssCon.getCatalogDAO();
		c.begin();
		try {
			QueryPlanner.invalidateCache(invalidate);
			c.persistToCatalog(newDatabase);
			
			// TODO:
			// start a transaction with the transaction manager so that DDL can be 
			// registered to back out the DDL we are about to execute in the 
			// event of a failure after the DDL is executed but before the txn is committed.

			WorkerRequest req = new WorkerCreateDatabaseRequest(ssCon.getNonTransactionalContext(), newDatabase, newDatabase.getMultitenantMode().isMT());
			wg.execute(MappingSolution.AllWorkers, req, resultConsumer);
			
			c.commit();
		} catch (Throwable t) {
			c.rollback(t);
			throw t;
		} finally {
			QueryPlanner.invalidateCache(invalidate);
		}

		// Tell the transaction manager that we have executed the catalog
		// changes successfully so that they can be removed from the 
		// recovery set
	}

	@Override
	public boolean requiresTransaction() {
		return false;
	}

	@Override
	public boolean requiresImplicitCommit() {
		return true;
	}
}

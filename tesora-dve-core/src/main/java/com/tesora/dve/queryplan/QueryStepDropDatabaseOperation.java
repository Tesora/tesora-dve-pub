// OS_STATUS: public
package com.tesora.dve.queryplan;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.WorkerDropDatabaseRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepDropDatabaseOperation extends QueryStepOperation {

	static Logger logger = Logger.getLogger( QueryStepDropDatabaseOperation.class );
	
	UserDatabase database;
	CacheInvalidationRecord record;
	
	public QueryStepDropDatabaseOperation(UserDatabase execCtxDBName, CacheInvalidationRecord cir) {
		this.database = execCtxDBName;
		this.record = cir;
	}
	
	/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {

		if (ssCon.hasActiveTransaction())
			throw new PEException("Cannot execute DDL within active transaction: DROP DATABASE " + database.getName());
		
		boolean mtdb = database.getMultitenantMode().isMT();
		
		CatalogDAO c = ssCon.getCatalogDAO();
		
		c.cleanupRollback();

		boolean success = false;
		boolean sideffects = false;
		int attempts = 0;
		while(!success) {
			attempts++;
			c.begin();
			try {

				QueryPlanner.invalidateCache(record);
				// do the changes to the catalog first because we may be able to  
				// restore the data if the ddl operation fails on the actual database
				UserDatabase db = c.findDatabase(database.getName());
				for(CatalogEntity dependentEntity : db.getDependentEntities(c)) {
					c.remove(dependentEntity);
				}
				c.remove(db);
				db.removeFromParent();
				
				// TODO:
				// start a transaction with the transaction manager so that DDL can be 
				// registered to back out the DDL we are about to execute in the 
				// event of a failure after the DDL is executed but before the txn is committed.

				if (!sideffects) {
					WorkerRequest req = new WorkerDropDatabaseRequest(ssCon.getNonTransactionalContext(), database.getName());
					wg.execute(MappingSolution.AllWorkers, req, resultConsumer);
					sideffects = true;
				}
				
				c.commit();
				success = true;
			} catch (Throwable t) {
				if (logger.isDebugEnabled())
					logger.debug("while dropping db " + database.getName(), t);
				if (mtdb) {
					c.rollback(t);
					throw t;					
				} else {
					c.retryableRollback(t);
					if (attempts > 10 || !AdaptiveMultitenantSchemaPolicyContext.canRetry(t)) {
						if (logger.isDebugEnabled())
							logger.debug("Giving up on drop db " + database.getName());
						throw t;
					} else {
						if (logger.isDebugEnabled())
							logger.debug("Retrying drop db " + database.getName());
					}
				}
			} finally {
				QueryPlanner.invalidateCache(record);
			}
		}						
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

package com.tesora.dve.queryplan;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.StorageGroup;
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
	
	public QueryStepDropDatabaseOperation(StorageGroup sg, UserDatabase execCtxDBName, CacheInvalidationRecord cir) throws PEException {
		super(sg);
		this.database = execCtxDBName;
		this.record = cir;
	}
	
	/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void executeSelf(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {

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
	public boolean requiresTransactionSelf() {
		return false;
	}

	@Override
	public boolean requiresImplicitCommit() {
		return true;
	}
}

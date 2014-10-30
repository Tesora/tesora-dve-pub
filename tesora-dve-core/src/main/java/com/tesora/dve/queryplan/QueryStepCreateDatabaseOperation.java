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
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.WorkerCreateDatabaseRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepCreateDatabaseOperation extends QueryStepOperation {

	static Logger logger = Logger.getLogger( QueryStepCreateDatabaseOperation.class );
	
	UserDatabase newDatabase;
	CacheInvalidationRecord invalidate;
	
	public QueryStepCreateDatabaseOperation(StorageGroup sg, UserDatabase db, CacheInvalidationRecord cir) throws PEException {
		super(sg);
		this.newDatabase = db;
		invalidate = cir;
	}
		
	/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void executeSelf(ExecutionState estate, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		if (estate.hasActiveTransaction())
			throw new PEException("Cannot execute DDL within active transaction: CREATE DATABASE " + newDatabase.getName());
		
		CatalogDAO c = estate.getCatalogDAO();
		c.begin();
		try {
			QueryPlanner.invalidateCache(invalidate);
			c.persistToCatalog(newDatabase);
			
			// TODO:
			// start a transaction with the transaction manager so that DDL can be 
			// registered to back out the DDL we are about to execute in the 
			// event of a failure after the DDL is executed but before the txn is committed.

			WorkerRequest req = new WorkerCreateDatabaseRequest(estate.getNonTransactionalContext(), newDatabase, newDatabase.getMultitenantMode().isMT());
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
	public boolean requiresTransactionSelf() {
		return false;
	}

	@Override
	public boolean requiresImplicitCommit() {
		return true;
	}
}

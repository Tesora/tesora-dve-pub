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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepCreateUserOperation extends QueryStepOperation {

	User user;
	CacheInvalidationRecord record;
	
	public QueryStepCreateUserOperation(User u, CacheInvalidationRecord record) {
		user = u;
		this.record = record;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
		if (ssCon.hasActiveTransaction())
			throw new PEException("Cannot execute CREATE USER " + user.getName() +  " within active transaction");
		
		// Send the catalog changes to the transaction manager so that it can
		// back them out in the event of a catastrophic failure

		// Add the catalog entries.  We have to do this before we execute any sql because the workers may
		// depend on the catalog being correct.  For instance, for a create database, the UserDatabase has to
		// exist in the catalog - otherwise the create database command is sent to the wrong database.
		CatalogDAO c = ssCon.getCatalogDAO();
		c.begin();
		try {
			QueryPlanner.invalidateCache(record);
			// do the changes to the catalog first because we may be able to  
			// restore the data if the ddl operation fails on the actual database
			c.persistToCatalog(user);
			
			// TODO:
			// start a transaction with the transaction manager so that DDL can be 
			// registered to back out the DDL we are about to execute in the 
			// event of a failure after the DDL is executed but before the txn is committed.

			// build two different stmts: the create user statement and the grant statement

            WorkerRequest req = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), Singletons.require(HostService.class).getDBNative().getCreateUserCommand(user));
			wg.execute(MappingSolution.AllWorkers, req, resultConsumer);

			c.commit();
		} catch (Throwable t) {
			c.rollback(t);
			throw t;
		} finally {
			QueryPlanner.invalidateCache(record);
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

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
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.WorkerAlterDatabaseRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepAlterDatabaseOperation extends QueryStepOperation {

	private final UserDatabase alteredDatabase;
	private final CacheInvalidationRecord invalidationRecord;

	public QueryStepAlterDatabaseOperation(StorageGroup sg, final UserDatabase alteredDatabase, final CacheInvalidationRecord invalidationRecord) throws PEException {
		super(sg);
		this.alteredDatabase = alteredDatabase;
		this.invalidationRecord = invalidationRecord;
	}

	@Override
	public void executeSelf(final ExecutionState estate, final WorkerGroup wg, final DBResultConsumer resultConsumer) throws Throwable {
		if (estate.hasActiveTransaction()) {
			throw new PEException("Cannot execute DDL within active transaction: ALTER DATABASE " + this.alteredDatabase.getName());
		}

		CatalogDAO c = estate.getCatalogDAO();
		c.begin();
		try {
			QueryPlanner.invalidateCache(this.invalidationRecord);
			c.persistToCatalog(this.alteredDatabase);

			// TODO:
			// start a transaction with the transaction manager so that DDL can
			// be
			// registered to back out the DDL we are about to execute in the
			// event of a failure after the DDL is executed but before the txn
			// is committed.

			final WorkerRequest request = new WorkerAlterDatabaseRequest(estate.getNonTransactionalContext(), this.alteredDatabase);
			wg.execute(MappingSolution.AllWorkers, request, resultConsumer);

			c.commit();
		} catch (Throwable t) {
			c.rollback(t);
			throw t;
		} finally {
			QueryPlanner.invalidateCache(this.invalidationRecord);
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

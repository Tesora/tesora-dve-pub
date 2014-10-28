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

import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.UserXid;
import com.tesora.dve.worker.WorkerGroup;

/**
 * {@link QueryStepRollbackTransactionOperation} is a <b>QueryStep</b> operation which
 * rolls back a transaction.
 *
 */
public class QueryStepRollbackTransactionOperation extends QueryStepOperation {

	@SuppressWarnings("unused")
	private UserXid xaXid;

	public QueryStepRollbackTransactionOperation(StorageGroup sg) throws PEException {
		super(sg);
	}
	
	public QueryStepRollbackTransactionOperation withXAXid(UserXid id) {
		this.xaXid = id;
		return this;
	}
	
	
	@Override
	public void executeSelf(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws PEException {
		ssCon.userRollbackTransaction();
		resultConsumer.rollback();
	}

	@Override
	public boolean requiresWorkers() {
		return false;
	}
}

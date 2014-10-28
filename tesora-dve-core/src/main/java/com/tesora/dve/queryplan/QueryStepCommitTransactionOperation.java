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
 * {@link QueryStepCommitTransactionOperation} is a <b>QueryStep</b> operation which
 * commits a transaction.
 *
 */
public class QueryStepCommitTransactionOperation extends QueryStepOperation {
	
	@SuppressWarnings("unused")
	private UserXid xaXid;
	@SuppressWarnings("unused")
	private boolean onePhase;

	public QueryStepCommitTransactionOperation(StorageGroup sg) throws PEException {
		super(sg);
	}
	
	public QueryStepCommitTransactionOperation withXAXid(UserXid xid, boolean onePhase) {
		this.xaXid = xid;
		this.onePhase = onePhase;
		return this;
	}
	
	@Override
	public void executeSelf(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
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

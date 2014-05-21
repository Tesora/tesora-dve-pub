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

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepOperationPrepareStatement extends QueryStepOperation {

	private SQLCommand toPrepare;
	private PersistentDatabase ctxDatabase;
	@SuppressWarnings("unused")
	private ProjectionInfo projection;
	
	public QueryStepOperationPrepareStatement(PersistentDatabase pdb, SQLCommand command, ProjectionInfo projInfo) {
		this.ctxDatabase = pdb;
		this.toPrepare = command;
		this.projection = projInfo;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
//		WorkerClientPrepareStatementRequest req = 
//				new WorkerClientPrepareStatementRequest(ssCon.getTransactionalContext(), ctxDatabase, toPrepare, projection);

		WorkerExecuteRequest req = new WorkerExecuteRequest(ssCon.getTransactionalContext(), toPrepare).onDatabase(ctxDatabase);
		wg.execute(MappingSolution.AnyWorker, req, resultConsumer);
	}

	@Override
	public boolean requiresTransaction() {
		return false;
	}
	
}

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

import java.util.Collection;
import java.util.concurrent.Future;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class QueryStepCreateTempTableOperation extends QueryStepOperation {

	private UserTable theTable;
	
	public QueryStepCreateTempTableOperation(StorageGroup sg, UserTable toCreate) throws PEException {
		super(sg);
		theTable = toCreate;
	}

	@Override
	public PersistentDatabase getContextDatabase() {
		return theTable.getDatabase();
	}
	
	@Override
	public void executeSelf(SSConnection ssCon, WorkerGroup wg,
			DBResultConsumer results) throws Throwable {
		String sqlCommand = theTable.getCreateTableStmt(true);
		WorkerRequest req = new WorkerExecuteRequest(
				ssCon.getNonTransactionalContext(), new SQLCommand(ssCon, sqlCommand))
				.onDatabase(theTable.getDatabase()).forDDL();
		ExecutionLogger logger = ssCon.getExecutionLogger().getNewLogger(
				"CreateTempTable");
		try {
			WorkerRequest preCleanupReq = new WorkerExecuteRequest(ssCon.getNonTransactionalContext(),
					UserTable.getDropTableStmt(ssCon, theTable.getName(), true))
					.onDatabase(theTable.getDatabase()).forDDL();
			wg.execute(MappingSolution.AllWorkers, preCleanupReq,DBEmptyTextResultConsumer.INSTANCE);
			Collection<Future<Worker>> f = wg.submit(MappingSolution.AllWorkers, req,
					DBEmptyTextResultConsumer.INSTANCE);
			wg.addCleanupStep(new WorkerExecuteRequest(ssCon.getNonTransactionalContext(), UserTable.getDropTableStmt(ssCon,
					theTable.getName(), false)).onDatabase(theTable.getDatabase()));
			WorkerGroup.syncWorkers(f);
		} finally {
			logger.end();
		}
	}
}

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

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public class TempTableGenerator {

	public static final TempTableGenerator DEFAULT_GENERATOR = new TempTableGenerator();
	
	static final boolean suppressTempDeletMode = Boolean.getBoolean("QueryStepMultiTupleRedistOperation.suppressTempDelete");
	
	public UserTable createTable(SSConnection ssCon, WorkerGroup targetWG, WorkerGroup cleanupWG,
			TempTableDeclHints tempHints, boolean useSystemTempTable, String tempTableName,
			PersistentDatabase database, ColumnSet metadata, DistributionModel tempDist) throws PEException {
		UserTable theTable = createTableFromMetadata(targetWG, tempHints, tempTableName, database, metadata, tempDist); 
//		System.out.println(tempTable.getName() + ".useSystemTempTable = " + useSystemTempTable + " on " + targetWG);
		String sqlCommand = buildCreateTableStatement(theTable,useSystemTempTable);
//		System.out.println(sqlCommand);
		WorkerRequest req = 
				new WorkerExecuteRequest(
						ssCon.getNonTransactionalContext(), new SQLCommand(sqlCommand)
						).onDatabase(database).forDDL();
		ExecutionLogger logger = ssCon.getExecutionLogger().getNewLogger("CreateTempTable");

		// We can't execute DDL on the targetWG if it is in a transaction
		WorkerGroup wgForDDL = targetWG;
		if (wgForDDL.activeTransactionCount() > 0)
			wgForDDL = WorkerGroupFactory.newInstance(ssCon, targetWG.getGroup(), database);

		try {
			if (!useSystemTempTable || suppressTempDeletMode) {
				WorkerRequest preCleanupReq = 
						new WorkerExecuteRequest(
								ssCon.getNonTransactionalContext(), UserTable.getDropTableStmt(tempTableName, true)
								).onDatabase(database).forDDL();
				wgForDDL.execute(MappingSolution.AllWorkers, preCleanupReq, DBEmptyTextResultConsumer.INSTANCE);
			}
			Collection<Future<Worker>> f = wgForDDL.submit(MappingSolution.AllWorkers, req, DBEmptyTextResultConsumer.INSTANCE);
			addCleanupStep(ssCon,theTable,database,cleanupWG);
			WorkerGroup.syncWorkers(f);
		} finally {
			if (wgForDDL != targetWG)
				WorkerGroupFactory.returnInstance(ssCon, wgForDDL);
			logger.end();
		}

		ssCon.setTempCleanupRequired(true);
		return theTable;
	}

	public UserTable createTableFromMetadata(WorkerGroup targetWG, 
			TempTableDeclHints tempHints, String tempTableName,
			PersistentDatabase database, ColumnSet metadata, DistributionModel tempDist) throws PEException {
		UserTable tempTable = UserTable.newTempTable((database instanceof UserDatabase ? (UserDatabase)database : null), metadata, tempTableName, tempDist);
		if (tempHints != null) tempHints.modify(tempTable);
		StorageGroup tempTableSG = targetWG.getGroup();
		tempTable.setStorageGroup(tempTableSG);
		return tempTable;
	}

	public String buildCreateTableStatement(UserTable theTable, boolean useSystemTempTable) throws PEException {
		return theTable.getCreateTableStmt(true, useSystemTempTable && !suppressTempDeletMode);
	}
	
	public void addCleanupStep(SSConnection ssCon, UserTable theTable, PersistentDatabase database, WorkerGroup cleanupWG) {
		if (!suppressTempDeletMode) {
			cleanupWG.addCleanupStep(
					new WorkerExecuteRequest(
							ssCon.getNonTransactionalContext(), UserTable.getDropTableStmt(theTable.getName(), false)
							).onDatabase(database));
		}
	}		
	
}

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

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepUpdateAllOperation extends QueryStepDMLOperation {

	static Logger logger = Logger.getLogger( QueryStepSelectAllOperation.class );
	
	DistributionModel distributionModel;
	SQLCommand command;

	public QueryStepUpdateAllOperation(StorageGroup sg, PersistentDatabase execCtxDBName, DistributionModel distModelOfTable, SQLCommand command) throws PEException {
		super(sg, execCtxDBName);
		if (command.isEmpty())
			throw new PEException("Cannot create QueryStep with empty SQL command");

		this.distributionModel = distModelOfTable;
		this.command = command;
	}

	public QueryStepUpdateAllOperation(StorageGroup sg, final SSConnection ssCon, PersistentDatabase execCtxDBName, DistributionModel distModelOfTable, String command)
			throws PEException {
		this(sg, execCtxDBName, distModelOfTable, new SQLCommand(ssCon, command));
	}
	/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void executeSelf(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		WorkerExecuteRequest req = new WorkerExecuteRequest(ssCon.getTransactionalContext(), command).onDatabase(database);
		resultConsumer.setRowAdjuster(distributionModel.getUpdateAdjuster());
		beginExecution();
		wg.execute(StaticDistributionModel.SINGLETON.mapForQuery(wg, command), req, resultConsumer);
		endExecution(resultConsumer.getUpdateCount());
	}
	
	@Override
	public String describeForLog() {
		StringBuilder buf = new StringBuilder();
		String rawSQL = command.getRawSQL();
		if (command.getStatementType() == StatementType.INSERT)
			rawSQL = rawSQL.substring(0,1024);
		buf.append("UpdateAll stmt=").append(rawSQL);
		return buf.toString();
	}
	
}

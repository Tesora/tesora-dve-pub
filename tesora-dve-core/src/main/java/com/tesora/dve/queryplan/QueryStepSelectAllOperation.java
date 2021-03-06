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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.worker.MysqlParallelResultConsumer;
import com.tesora.dve.worker.WorkerGroup;

/**
 * {@link QueryStepSelectAllOperation} is a <b>QueryStep</b> operation which
 * executes a sql query against a {@link PersistentGroup}, returning the results
 * of the query.
 *
 */
@XmlType(name="QueryStepSelectAllOperation")
public class QueryStepSelectAllOperation extends QueryStepResultsOperation {

	static Logger logger = Logger.getLogger( QueryStepSelectAllOperation.class );
	
	@XmlElement(name="DistributionModel")
	DistributionModel distributionModel;
	@XmlElement(name="SqlCommand")
	SQLCommand command;
	
	/**
	 * Constructs a <b>QueryStepSelectAllOperation</b> to execute the provided 
	 * sql query against the database specified by <em>execCtxDBName</em>.
	 * 
	 * @param execCtxDBName database in which the <em>command</em> will be executed
	 * @param command sql query to execute
	 * @throws PEException 
	 */
	public QueryStepSelectAllOperation(StorageGroup sg, PersistentDatabase execCtxDBName, DistributionModel distModel, SQLCommand command) throws PEException {
		super(sg, execCtxDBName);
		if (command.isEmpty())
			throw new PEException("Cannot create QueryStep with empty SQL command");

		this.distributionModel= distModel;
		this.command = command;
	}

	public QueryStepSelectAllOperation(StorageGroup sg, final SSConnection ssCon, PersistentDatabase execCtxDBName, DistributionModel distModel, String command)
			throws PEException {
		this(sg, execCtxDBName, distModel, new SQLCommand(ssCon, command));
	}

		/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void executeSelf(ExecutionState estate, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		resultConsumer.setResultsLimit(getResultsLimit());
		SQLCommand bound = bindCommand(estate,command);
		WorkerExecuteRequest req = new WorkerExecuteRequest(estate.getConnection().getTransactionalContext(), bound).onDatabase(database);
		beginExecution();
		wg.execute(distributionModel.mapForQuery(wg, bound), req, resultConsumer);
		if (resultConsumer instanceof MysqlParallelResultConsumer) 
			endExecution(((MysqlParallelResultConsumer)resultConsumer).getNumRowsAffected());
	}

	@Override
	public boolean requiresTransactionSelf() {
		return false;
	}
	
	@Override
	public String describeForLog() {
		StringBuilder buf = new StringBuilder();
		buf.append("SelectAll stmt=").append(command.getRawSQL());
		return buf.toString();
	}
}

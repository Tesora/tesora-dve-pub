// OS_STATUS: public
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

import javax.xml.bind.annotation.XmlType;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.worker.WorkerGroup;

/**
 * <b>QueryStepInsertOperation</b> is a <b>QueryStep</b> operation which inserts
 * a single row into a table.  The provided <em>distValue</em> is used to determine
 * which {@link PersistentSite} the row should be inserted into.
 *
 */
@XmlType(name="QueryStepInsertOperation")
public class QueryStepInsertByKeyOperation extends QueryStepDMLOperation {

	static Logger logger = Logger.getLogger( QueryStepInsertByKeyOperation.class );

	SQLCommand command;
	IKeyValue distValue;
	
	/**
	 * Constructs a <b>QueryStepInsertOperation</b> to insert a row into a table
	 * on the database identified by <em>execCtxDBName</em>.  
	 * 
	 * @param execCtxDBName database on which to insert the row
	 * @param distValue <em>distValue</em> which determines {@link PersistentSite} to hold row.
	 * @param command sql command to actually insert the row
	 * @throws PEException 
	 */
	public QueryStepInsertByKeyOperation(PersistentDatabase execCtxDBName, IKeyValue distValue, SQLCommand command) throws PEException {
		super(execCtxDBName);
		if (command.isEmpty())
			throw new PEException("Cannot create QueryStep with empty SQL command");

		this.command = command;
		this.distValue = distValue;
	}

	// compatibility ctor
	public QueryStepInsertByKeyOperation(PersistentDatabase execCtxDB, IKeyValue distValue, String command) throws PEException {
		this(execCtxDB, distValue, new SQLCommand(command));
	}
	
	/**
	 * Called by <b>QueryStep</b> to do the insert operation.
	 * <p/>
	 * The site on which
	 * the row will be inserted in determined by mapping the provided <em>distValue</em>
	 * through the {@link DistributionModel} associated with the table.  <em>command</em>
	 * is executed to actually insert the row.
	 * */
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		beginExecution();
		executeInsertByKey(ssCon, wg, resultConsumer, database, command, distValue);
		endExecution(resultConsumer.getUpdateCount());
	}

	public static void executeInsertByKey(SSConnection ssCon,
			WorkerGroup wg, DBResultConsumer resultConsumer, PersistentDatabase database, SQLCommand command, 
			IKeyValue distValue) throws Throwable {
		DistributionModel dm = distValue.getDistributionModel();
		
		WorkerExecuteRequest req = new WorkerExecuteRequest(ssCon.getTransactionalContext(), command).onDatabase(database);
		WorkerGroup.MappingSolution mappingSolution = dm.mapKeyForInsert(ssCon.getCatalogDAO(), wg.getGroup(), distValue);
		
		resultConsumer.setRowAdjuster(dm.getInsertAdjuster());
		wg.execute(mappingSolution, req, resultConsumer);
	}
	
	@Override
	public boolean requiresTransaction() {
		return false;
	}

	@Override
	public String describeForLog() {
		StringBuilder buf = new StringBuilder();
		buf.append("InsertByKey key=").append(distValue).append(", stmt=").append(command.getRawSQL());
		return buf.toString();
	}
}

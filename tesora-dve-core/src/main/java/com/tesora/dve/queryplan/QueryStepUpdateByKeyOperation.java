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

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

@XmlType(name="QueryStepUpdateByKeyOperation")
public class QueryStepUpdateByKeyOperation extends QueryStepDMLOperation {

	static Logger logger = Logger.getLogger( QueryStepUpdateByKeyOperation.class );

	SQLCommand command;
	IKeyValue distValue;
	
	public QueryStepUpdateByKeyOperation(PersistentDatabase execCtxDBName, IKeyValue distValue, SQLCommand sql) throws PEException {
		super(execCtxDBName);
		if (sql.isEmpty())
			throw new PEException("Cannot create QueryStep with empty SQL command");

		this.command = sql;
		this.distValue = distValue;
	}

	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		DistributionModel dm = distValue.getDistributionModel();
		
		WorkerGroup.MappingSolution mappingSolution = dm.mapKeyForUpdate(ssCon.getCatalogDAO(), wg.getGroup(), distValue);

        final boolean savepointRequired = ssCon.getTransId() != null && mappingSolution.equals(MappingSolution.AllWorkers)
				&& "5.6".equals(Singletons.require(HostService.class).getDveVersion(ssCon));

		WorkerExecuteRequest req =
				new WorkerExecuteRequest(ssCon.getTransactionalContext(), command).
				onDatabase(database).withLockRecovery(savepointRequired);
		
		resultConsumer.setRowAdjuster(dm.getUpdateAdjuster());
		beginExecution();
		wg.execute(mappingSolution, req, resultConsumer);
		endExecution(resultConsumer.getUpdateCount());
	}

	@Override
	public boolean requiresTransaction() {
		return false;
	}

	@Override
	public String describeForLog() {
		StringBuilder buf = new StringBuilder();
		String rawSQL = command.getRawSQL();
		if (command.getStatementType() == StatementType.INSERT)
			rawSQL = rawSQL.substring(0,1024);
		buf.append("UpdateByKey key=").append(distValue).append(", stmt=").append(rawSQL);
		return buf.toString();
	}
	
}

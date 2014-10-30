package com.tesora.dve.sql.statement.session;

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


import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.ExecutionState;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.session.AnalyzeKeysStatement.LoadKeyInfo;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.worker.WorkerGroup;

public class AnalyzeTablesStatement extends TableMaintenanceStatement {

	public AnalyzeTablesStatement(MaintenanceOptionType option,List<TableInstance> tabs) {
		super(MaintenanceCommandType.ANALYZE,option,tabs);
	}
	
	@Override
	public boolean isPassthrough() {
		return false;
	}
	
	// have to use an adhoc operation - after this runs we need to fire off an event to do the info schema query
	
	@Override
	public void plan(final SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		// so we have to do adhoc operations - each one is for a particular storage group & database
		// and fires off the event;
		MultiMap<Database<?>,TableInstance> byDB = new MultiMap<Database<?>,TableInstance>();
		for(TableInstance ti : tableInstanceList) {
			byDB.put(ti.getAbstractTable().getDatabase(pc), ti);
		}
		for(final Database<?> db : byDB.keySet()) {
			List<TableInstance> tabs = (List<TableInstance>) byDB.get(db);
			// build the sql
			AnalyzeTablesStatement temp = new AnalyzeTablesStatement(option, tabs);
			StringBuilder buf = new StringBuilder();
            Singletons.require(HostService.class).getDBNative().getEmitter().emitTableMaintenanceStatement(pc, temp, buf, -1);
			// we only allow on a single Persistent Group, so grab the first one
			PEPersistentGroup sg = tabs.get(0).getAbstractTable().getPersistentStorage(pc);
			final String sql = buf.toString();
			buf = new StringBuilder();
			AnalyzeKeysStatement aks = new AnalyzeKeysStatement(tabs);
            Singletons.require(HostService.class).getDBNative().getEmitter().emitAnalyzeKeysStatement(pc, aks, buf, -1);
			final String aksStmt = buf.toString();
			es.append(new TransientSessionExecutionStep(db, sg, sql, false, true, new AdhocOperation() {

				@Override
				public void execute(ExecutionState estate, WorkerGroup wg,
						DBResultConsumer resultConsumer) throws Throwable {
					resultConsumer.setResultsLimit(Long.MAX_VALUE);
					SQLCommand command = new SQLCommand(pc, sql);
					WorkerExecuteRequest req = new WorkerExecuteRequest(estate.getConnection().getTransactionalContext(), command).onDatabase(db);
					wg.execute(StaticDistributionModel.SINGLETON.mapForQuery(wg, command), req, resultConsumer);
                    Singletons.require(HostService.class).submit(new LoadKeyInfo("use " + db.getName().getSQL(), aksStmt));
				}
			}));
		}		
	}

}

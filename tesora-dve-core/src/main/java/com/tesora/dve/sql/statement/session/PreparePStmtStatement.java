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

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.mysql.MysqlPrepareStatementDiscarder;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.parser.InitialInputState;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.PreparePlanningResult;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.PlanCacheUtils;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionPlanOptions;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.worker.WorkerGroup;

public class PreparePStmtStatement extends PStmtStatement {

	private String stmt;
	
	public PreparePStmtStatement(UnqualifiedName unq, String stmt) {
		super(unq);
		this.stmt = stmt;
	}
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		final SchemaContext indep = SchemaContext.createContext(sc);
		final PreparePlanningResult prepResult = 
				(PreparePlanningResult) InvokeParser.preparePlan(indep, new InitialInputState(stmt), sc.getOptions(), getName().get());
		final String pstmtId = getName().get();

		// the effective group is the one from the embedded plan
		final ExecutionPlan ep = prepResult.getPlans().get(0);
		ExecutionStep subes = (ExecutionStep) ep.getSequence().getSteps().get(0);
		
		es.append(new TransientSessionExecutionStep(subes.getDatabase(),subes.getPEStorageGroup(),"", false, true, new AdhocOperation() {

			@Override
			public void execute(SSConnection ssCon, WorkerGroup wg,
					DBResultConsumer resultConsumer) throws Throwable {
				// convert to a plan
				List<QueryStepOperation> steps = ep.schedule(new ExecutionPlanOptions(), ssCon, indep);
				QueryStepOperation qso = steps.get(0);
				MysqlPrepareStatementDiscarder discarder = new MysqlPrepareStatementDiscarder();
				qso.executeSelf(ssCon, wg, discarder);
				if (discarder.isSuccessful())
					PlanCacheUtils.registerPreparedStatementPlan(indep, prepResult.getCachedPlan(),
							prepResult.getOriginalSQL(), ssCon.getConnectionId(), pstmtId, false);
			}
			
		}));
	}
}

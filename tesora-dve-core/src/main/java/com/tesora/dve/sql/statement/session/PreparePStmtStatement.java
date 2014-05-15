// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import java.util.List;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.mysql.MysqlPrepareStatementDiscarder;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.parser.InitialInputState;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.PreparePlanningResult;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.PlanCacheUtils;
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
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {
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
				List<QueryStep> steps = ep.schedule(new ExecutionPlanOptions(), ssCon, indep);
				QueryStepOperation qso = steps.get(0).getOperation();
				MysqlPrepareStatementDiscarder discarder = new MysqlPrepareStatementDiscarder();
				qso.execute(ssCon, wg, discarder);
				if (discarder.isSuccessful())
					PlanCacheUtils.registerPreparedStatementPlan(indep, prepResult.getCachedPlan(),
							prepResult.getOriginalSQL(), ssCon.getConnectionId(), pstmtId, false);
			}
			
		}));
	}
}

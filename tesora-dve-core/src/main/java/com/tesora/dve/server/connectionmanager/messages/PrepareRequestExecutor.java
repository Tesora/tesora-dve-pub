// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import java.nio.charset.Charset;

import com.tesora.dve.db.mysql.MysqlPrepareParallelConsumer;
import com.tesora.dve.queryplan.PreparedPlan;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.queryplan.QueryPlanner;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class PrepareRequestExecutor {
	
	public static void execute(SSConnection ssCon, MysqlPrepareParallelConsumer resultConsumer,
			String pstmtId, Charset cs, byte[] command) throws Throwable {
		
		PreparedPlan prep = QueryPlanner.prepareStatement(command, cs, ssCon, pstmtId);
		QueryPlan plan = prep.getMetadataPlan();
		plan.executeStep(ssCon, resultConsumer);

		if (resultConsumer.isSuccessful())
			QueryPlanner.registerPreparedStatement(ssCon, pstmtId, prep);
	}

//	@Override
//	public ResponseMessage execute(SSConnection connMgr, Object message) throws Throwable {
//		PrepareRequest pr = (PrepareRequest) message;
//
//		Charset cs = ExecuteRequestExecutor.getCharset(connMgr, pr);
//		
//		QueryPlan plan = null;
//		SSStatement stmt = connMgr.getStatementForNewQuery(pr.getStatementId());
//		stmt.clearQueryPlan(); // so we can re-use resources
//		PreparedPlan prep = QueryPlanner.prepareStatement(pr.getCommand(), cs, connMgr,pr.getStatementId());
//		plan = prep.getMetadataPlan();
//		stmt.setQueryPlan(plan);
//		if (!plan.getSteps().isEmpty()) {
//			plan.executeStep(connMgr);
//		}
//
//		// the plan will have a result set, and it will be our special ResultCollectorPrepareMetadata thingy
//		ResultCollectorPrepareMetadata rcm = (ResultCollectorPrepareMetadata) plan.getResultCollector();
//		
//		// the ordinary metadata is for the result set that would occur upon executing the prep stmt
//		// the special one is for the parameters
//		ColumnSet resultsMetadata = rcm.getMetadata();
//		ColumnSet paramsMetadata = rcm.getParamsMetadata();
//		
//		QueryPlanner.registerPreparedStatement(connMgr, pr.getStatementId(), prep);
//		
//		ResponseMessage prepResp = new PrepareResponse(paramsMetadata, resultsMetadata).success();
//		return prepResp;
//	}
}
// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;


import java.util.List;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.queryplan.QueryPlanner;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.statistics.manager.LogGlobalStatisticRequest;
import com.tesora.dve.server.statistics.manager.StatisticsRequest;

public class ExecutePreparedStatementRequestExecutor {

	private static Logger logger = Logger.getLogger( ExecutePreparedStatementRequestExecutor.class );

	public static void execute(SSConnection connMgr, String stmtId, List<String> params, DBResultConsumer resultConsumer) throws Throwable {
		long stepStartTime = System.currentTimeMillis();

		QueryPlan plan = QueryPlanner.buildPreparedPlan(connMgr, stmtId, params);
		plan.executeStep(connMgr, resultConsumer);

		if (logger.isDebugEnabled())
			logger.debug("ExecuteRequest/c(" + connMgr.getName() + "): yielded results: " + resultConsumer.hasResults()
					+ " (update count = " + resultConsumer.getUpdateCount() + ")");

		StatisticsRequest req =
				new LogGlobalStatisticRequest(resultConsumer.getUpdateCount(),
						System.currentTimeMillis() - stepStartTime);
        connMgr.send(connMgr.newEnvelope(req).to(Singletons.require(HostService.class).getStatisticsManagerAddress()));
	}

}

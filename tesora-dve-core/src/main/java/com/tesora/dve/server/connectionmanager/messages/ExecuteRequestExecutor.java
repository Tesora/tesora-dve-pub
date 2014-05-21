// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

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


import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.comms.client.messages.ExecuteResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.queryplan.QueryPlanner;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.statistics.manager.LogGlobalStatisticRequest;
import com.tesora.dve.server.statistics.manager.StatisticsRequest;
import com.tesora.dve.sql.parser.InputState;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.MysqlTextResultCollector;

public class ExecuteRequestExecutor {
	private static Logger logger = Logger.getLogger( ExecuteRequestExecutor.class );

	public static ResponseMessage execute(SSConnection ssCon, DBResultConsumer resultConsumer, byte[] command) 
			throws PEException, Throwable {
		long stepStartTime = System.currentTimeMillis();
		long totalUpdateCount = 0;		
		InputState continuationState = null;
		
		do {
			Pair<QueryPlan,InputState> results = null;
			if (continuationState == null)
				results = QueryPlanner.computeQueryPlan(command, ssCon.getClientCharSet().getJavaCharset(), ssCon);
			else
				results = QueryPlanner.computeQueryPlan(continuationState, ssCon);
			QueryPlan plan = results.getFirst();
			try {
				continuationState = results.getSecond();
				if (!plan.getSteps().isEmpty()) {
					ExecutionLogger slowLogger = ssCon.getNewPlanLogger(plan);
					try {
						plan.executeStep(ssCon, resultConsumer);
					} finally {
						slowLogger.end();
					}
				}
			} finally {
				plan.close();
			}

			if (logger.isDebugEnabled())
				logger.debug("ExecuteRequest/c(" + ssCon.getName() + "): yielded results: " + resultConsumer.hasResults()
						+ " (update count = " + resultConsumer.getUpdateCount() + ")");
			
			totalUpdateCount += resultConsumer.getUpdateCount();
			
			StatisticsRequest req =
					new LogGlobalStatisticRequest(resultConsumer.getUpdateCount(),
							System.currentTimeMillis() - stepStartTime);
            ssCon.send(ssCon.newEnvelope(req).to(Singletons.require(HostService.class).getStatisticsManagerAddress()));

		} while (continuationState != null);
		
		ColumnSet metaData = null;
		if (resultConsumer instanceof MysqlTextResultCollector) {
			metaData = ((MysqlTextResultCollector)resultConsumer).getColumnSet();
		}
		return new ExecuteResponse(resultConsumer.hasResults(), totalUpdateCount, metaData, ssCon.getLastInsertedId(),
				ssCon.hasActiveTransaction(),ssCon.getMessageManager().getNumberOfMessages()).success();
	}
}

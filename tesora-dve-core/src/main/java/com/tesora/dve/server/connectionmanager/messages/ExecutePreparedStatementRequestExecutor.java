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

	public static void execute(SSConnection connMgr, String stmtId, List<Object> params, DBResultConsumer resultConsumer) throws Throwable {
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

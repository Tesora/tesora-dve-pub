// OS_STATUS: public
package com.tesora.dve.server.statistics.manager;

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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.server.connectionmanager.log.ShutdownLog;
import com.tesora.dve.server.statistics.*;
import org.apache.log4j.Logger;

import com.tesora.dve.common.RemoteException;
import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.worker.agent.Agent;

public class StatisticsManager extends Agent implements StatisticsManagerMBean {

	Logger logger = Logger.getLogger(StatisticsManager.class);

	static SiteStatKey globalQuerySsk = new SiteStatKey(SiteStatKey.SiteType.GLOBAL, null, OperationClass.QUERY);
	static SiteStatKey globalUpdateSsk = new SiteStatKey(SiteStatKey.SiteType.GLOBAL, null, OperationClass.UPDATE);

	StatisticsTracker globalStatsQuery = new StatisticsTracker();
	StatisticsTracker globalStatsUpdate = new StatisticsTracker();
	Map<SiteStatKey, StatisticsTracker> siteStats = new ConcurrentHashMap<SiteStatKey, StatisticsTracker>();

	public StatisticsManager() throws PEException {
		super("StatisticsManager");
	}

	@Override
	public void onMessage(Envelope e) throws PEException {
		Object payload = e.getPayload();

		if (payload instanceof StatisticsRequest) {
			StatisticsRequest m = (StatisticsRequest) payload;
			ResponseMessage resp;
			try {
				resp = m.executeRequest(this);
			} catch (Exception ex) {
				resp = new GenericResponse().setException(new RemoteException(getName(), ex));
			}
			if (resp != null) {
				try {
					returnResponse(e, resp);
				} catch (Exception ex) {
					returnResponse(e, new GenericResponse().setException(new RemoteException(getName(), ex)));
				}
			}
		} else {
			throw new PEException("StatisticsManager received message of invalid type (" + payload.toString() + ")");
		}
	}

	void logGlobalQuery(int responseTime) {
		globalStatsQuery.logQuery(responseTime);
	}

	void logGlobalUpdate(int responseTime) {
		globalStatsUpdate.logQuery(responseTime);
	}

	void logSiteStatistic(SiteStatKey.SiteType type, String site, OperationClass opClass, int responseTime) {
		SiteStatKey key = new SiteStatKey(type, site, opClass);
		if (!siteStats.containsKey(key))
			siteStats.put(key, new StatisticsTracker());
		siteStats.get(key).logQuery(responseTime);
	}

	public void printStats() {
		globalStatsQuery.printStats("GlobalQuery");
		globalStatsUpdate.printStats("GlobalUpdate");
		for (Entry<SiteStatKey, StatisticsTracker> e : siteStats.entrySet()) {
			e.getValue().printStats("Site " + e.getKey() + ": ");
		}
		System.out.println();
	}

	public void shutdown() {
		
		globalStatsQuery = null;
		globalStatsUpdate = null;
		if(siteStats != null)
			siteStats.clear();
		siteStats = null;

		try {
			super.close();
		} catch (PEException e) {
			ShutdownLog.logShutdownError("Error shutting down " + getClass().getSimpleName(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.tesora.dve.server.statistics.StatisticsMBean#getStatistics()
	 */
	public ServerStatistics getStatistics() {
		TimingSet gQuery = globalStatsQuery.getTimingSet(globalQuerySsk);
		TimingSet gUpdate = globalStatsUpdate.getTimingSet(globalUpdateSsk);
		ServerStatistics stat = new ServerStatistics(gQuery, gUpdate);
		for (Entry<SiteStatKey, StatisticsTracker> ss : siteStats.entrySet()) {
			stat.addSiteTiming(ss.getValue().getTimingSet(ss.getKey()));
		}
		return stat;
	}

}

// OS_STATUS: public
package com.tesora.dve.server.statistics;

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

import com.tesora.dve.server.statistics.SiteStatKey;
import com.tesora.dve.server.statistics.TimingSet;

public class StatisticsTracker {
	
	StatisticsAccumulator queryOneSecond = new RollingStatisticsAccumulator(10,100);
	StatisticsAccumulator queryTenSecond = new RollingStatisticsAccumulator(10,1000);
	StatisticsAccumulator queryOneMinute = new RollingStatisticsAccumulator(10,6000);
	StatisticsAccumulator cumulative = new CumulativeStatisticsAccumulator();
	
	synchronized public void logQuery(int responseTime) {
		queryOneSecond.addDatum(responseTime);
		queryTenSecond.addDatum(responseTime);
		queryOneMinute.addDatum(responseTime);
		cumulative.addDatum(responseTime);
	}
	
	synchronized public TimingSet getTimingSet(SiteStatKey ssk) {
		return new TimingSet(ssk, queryOneSecond.getTimingValue(), 
				queryTenSecond.getTimingValue(), queryOneMinute.getTimingValue(),
				cumulative.getTimingValue());
	}

	public void printStats(String title) {
		System.out.println(title 
				+ "\t" + queryOneSecond.getAverageTime() + "/" + queryOneSecond.getTransactionsPerSecond()
				+ "\t" + queryTenSecond.getAverageTime() + "/" + queryTenSecond.getTransactionsPerSecond() 
				+ "\t" + queryOneMinute.getAverageTime() + "/" + queryOneMinute.getTransactionsPerSecond()
				+ "\t" + cumulative.getTimingValue().getResponseTimeMS() + "/" + cumulative.getTimingValue().getSampleSize());
	}
}

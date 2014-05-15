// OS_STATUS: public
package com.tesora.dve.server.statistics;

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

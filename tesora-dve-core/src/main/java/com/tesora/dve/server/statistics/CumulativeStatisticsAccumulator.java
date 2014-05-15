// OS_STATUS: public
package com.tesora.dve.server.statistics;

import com.tesora.dve.server.statistics.TimingValue;

public class CumulativeStatisticsAccumulator implements StatisticsAccumulator {

	long cumulativeResponseTime;
	long count;
	
	@Override
	public void addDatum(int responseTimeMS) {
		cumulativeResponseTime += (responseTimeMS == 0) ? 1 : responseTimeMS;
		count++;
	}

	@Override
	public float getAverageTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getAverageValue() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getTransactionsPerSecond() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isDebugMode() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setDebugMode(boolean debugMode) {
		// TODO Auto-generated method stub

	}

	@Override
	public TimingValue getTimingValue() {
		return new TimingValue(cumulativeResponseTime, count);
	}

}

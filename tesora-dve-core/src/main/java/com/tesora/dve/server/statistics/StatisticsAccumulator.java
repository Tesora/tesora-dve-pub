// OS_STATUS: public
package com.tesora.dve.server.statistics;

import com.tesora.dve.server.statistics.TimingValue;

public interface StatisticsAccumulator {

	public abstract void addDatum(int responseTimeMS);

	public abstract float getAverageTime();

	public abstract float getAverageValue();

	public abstract int getTransactionsPerSecond();

	public abstract boolean isDebugMode();

	public abstract void setDebugMode(boolean debugMode);

	public abstract TimingValue getTimingValue();

}
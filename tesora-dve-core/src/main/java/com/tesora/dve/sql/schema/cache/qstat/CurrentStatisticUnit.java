package com.tesora.dve.sql.schema.cache.qstat;

import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.groupmanager.QueryStatisticsMessage;
import com.tesora.dve.sql.schema.cache.qstat.RuntimeQueryStatistic.RuntimeQueryCacheKey;

class CurrentStatisticUnit implements StatisticMeasurement, StepExecutionStatistics {

	private final AtomicLong rows;
	private final AtomicLong calls;
	private final AtomicLong time;
	
	public CurrentStatisticUnit() {
		this.rows = new AtomicLong(0);
		this.calls = new AtomicLong(0);
		this.time = new AtomicLong(0);
	}
		
	@Override
	public long getAvgRows() {
		if (calls.get() == 0) return 0;
		return Math.round(rows.get() / calls.get());
	}

	@Override
	public long getCalls() {
		return calls.get();
	}

	@Override
	public long getExecTime() {
		return this.time.get();
	}

	public void reset() {
		this.rows.set(0);
		this.calls.set(0);
		this.time.set(0);
	}
	
	public QueryStatisticsMessage buildMessage(RuntimeQueryCacheKey ck) {
		return new QueryStatisticsMessage(ck,buildHistoricalUnit());
	}
	
	public HistoricalStatisticUnit buildHistoricalUnit() {
		return new HistoricalStatisticUnit(getAvgRows(),calls.get());
	}

	@Override
	public void onExecution(long elapsed, long rows) {
		this.rows.getAndAdd(rows);
		this.time.getAndAdd(elapsed);
		this.calls.incrementAndGet();
	}
}
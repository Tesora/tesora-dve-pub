package com.tesora.dve.sql.schema.cache.qstat;

import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.groupmanager.QueryStatisticsMessage;
import com.tesora.dve.sql.schema.cache.qstat.RuntimeQueryStatistic.RuntimeQueryCacheKey;

class CurrentStatisticUnit implements StatisticMeasurement {

	private final AtomicLong rows;
	private final AtomicLong calls;
	
	public CurrentStatisticUnit() {
		this.rows = new AtomicLong(0);
		this.calls = new AtomicLong(0);
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

	public long onMeasurement(long rows) {
		this.rows.getAndAdd(rows);
		return calls.incrementAndGet();
	}
	
	public void reset() {
		this.rows.set(0);
		this.calls.set(0);
	}
	
	public QueryStatisticsMessage buildMessage(RuntimeQueryCacheKey ck) {
		return new QueryStatisticsMessage(ck,buildHistoricalUnit());
	}
	
	public HistoricalStatisticUnit buildHistoricalUnit() {
		return new HistoricalStatisticUnit(getAvgRows(),calls.get());
	}
}
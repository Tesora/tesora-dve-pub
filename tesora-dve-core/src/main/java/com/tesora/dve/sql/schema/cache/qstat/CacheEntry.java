package com.tesora.dve.sql.schema.cache.qstat;

import com.tesora.dve.common.catalog.PersistentQueryStatistic;
import com.tesora.dve.groupmanager.QueryStatisticsMessage;
import com.tesora.dve.sql.schema.cache.qstat.RuntimeQueryStatistic.RuntimeQueryCacheKey;

class CacheEntry {
	
	private final CurrentStatisticUnit current;
	private final HistoricalQueryStatistics global;
	
	public CacheEntry() {
		this.current = new CurrentStatisticUnit();
		this.global = new HistoricalQueryStatistics();
	}
	
	public long getCurrentAvg() {
		return (current.getAvgRows() + global.getCurrentAvg())/2;
	}
			
	public boolean onMeasurement(long rows) {
		long c = this.current.onMeasurement(rows);
		return (c > RuntimeQueryStatisticsCache.window);
	}

	public void onHistory(HistoricalStatisticUnit hsu) {
		global.onHistoricalUnit(hsu);
	}
	
	public QueryStatisticsMessage buildMessage(RuntimeQueryCacheKey ck) {
		QueryStatisticsMessage out = current.buildMessage(ck);
		current.reset();
		return out;
	}
	
	public void updatePersistent(PersistentQueryStatistic pqs) {
		// we only take the global now
		pqs.updateMeasurement(global.getCurrentAvg(), global.getCurrentCalls());
	}
}
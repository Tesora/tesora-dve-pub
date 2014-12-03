package com.tesora.dve.sql.schema.cache.qstat;

import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.groupmanager.QueryStatisticsMessage;
import com.tesora.dve.sql.schema.cache.qstat.RuntimeQueryStatistic.RuntimeQueryCacheKey;

public class RuntimeQueryStatisticsCache {

	// will make this configurable soon
	private static final long window = 10;
	
	private final ConcurrentHashMap<RuntimeQueryCacheKey,CacheEntry> entries;
	
	public RuntimeQueryStatisticsCache() {
		this.entries = new ConcurrentHashMap<RuntimeQueryCacheKey,CacheEntry>();
	}

	public long getCurrentAvg(RuntimeQueryCacheKey rqce) {
		CacheEntry e = entries.get(rqce);
		if (e == null) return -1;
		return e.getCurrentAvg();
	}
	
	public void onMeasurement(RuntimeQueryCacheKey rqck, long rows) {
		CacheEntry e = entries.get(rqck);
		if (e == null) {
			// it's new
			e = new CacheEntry();
			entries.put(rqck,e);
		}
		if (e.onMeasurement(rows)) {
			QueryStatisticsMessage qsm = e.buildMessage(rqck);
			// do something to send this
		}
	}
	
	public void onHistoricalUnit(RuntimeQueryCacheKey rqck, HistoricalStatisticUnit unit) {
		CacheEntry e = entries.get(rqck);
		if (e == null) {
			e = new CacheEntry();
			entries.put(rqck,e);
		}
		e.onHistory(unit);
	}
	
	private static class CacheEntry {
		
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
			return (c > window);
		}

		public void onHistory(HistoricalStatisticUnit hsu) {
			global.onHistoricalUnit(hsu);
		}
		
		public QueryStatisticsMessage buildMessage(RuntimeQueryCacheKey ck) {
			QueryStatisticsMessage out = current.buildMessage(ck);
			current.reset();
			return out;
		}
	}
}

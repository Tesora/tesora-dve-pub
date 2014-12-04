package com.tesora.dve.sql.schema.cache.qstat;

import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.common.catalog.PersistentQueryStatistic;
import com.tesora.dve.groupmanager.CacheInvalidationMessage;
import com.tesora.dve.groupmanager.GroupTopicPublisher;
import com.tesora.dve.groupmanager.QueryStatisticsMessage;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.cache.qstat.RuntimeQueryStatistic.RuntimeQueryCacheKey;

public class RuntimeQueryStatisticsCache {

	// will make this configurable soon
	static final long window = 10;
	
	private final ConcurrentHashMap<RuntimeQueryCacheKey,CacheEntry> entries;
	
	public RuntimeQueryStatisticsCache() {
		this.entries = new ConcurrentHashMap<RuntimeQueryCacheKey,CacheEntry>();
	}

	public long getCurrentAvg(RuntimeQueryCacheKey rqce) {
		CacheEntry e = entries.get(rqce);
		if (e == null) return -1;
		return e.getCurrentAvg();
	}
	
	public void updatePersistent(RuntimeQueryCacheKey rqck, PersistentQueryStatistic pqs) {
		CacheEntry e = entries.get(rqck);
		if (e == null) return;
		e.updatePersistent(pqs);
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
	        Singletons.require(GroupTopicPublisher.class).publish(qsm);
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
}

// OS_STATUS: public
package com.tesora.dve.variable.status;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.sql.schema.cache.CacheLimits;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaCache;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;

public class CacheStatusVariableHandler extends StatusVariableHandler {

	@Override
	public String getValue(CatalogDAO c, String name) throws PENotFoundException {
		SchemaCache global = SchemaSourceFactory.peekGlobalCache();
		CacheLimits limits = SchemaSourceFactory.peekCacheLimits();

		if (global == null)
			return "0";

		// The format of defaultValue should be <Cache Segment>,<metric>
		int index = defaultValue.indexOf(',');

		if (index == -1)
			throw new PENotFoundException("Unable to parse status variable " + name + " defaultValue " + defaultValue);

		String segment = defaultValue.substring(0, index).trim();
		String metric = defaultValue.substring(index + 1).trim();

		CacheSegment cs = CacheSegment.valueOf(segment);

		if ("size".equalsIgnoreCase(metric))
			return Long.toString(global.getCacheSize(cs));

		if ("utilization".equalsIgnoreCase(metric)) {
			int limit = limits.getLimit(cs);
			return limit == 0 ? "0" : Long.toString((global.getCacheSize(cs) * 100) / limit);
		}

		if ("evictions".equalsIgnoreCase(metric))
			return Long.toString(global.getCacheStats(cs).evictionCount());

		if ("load_time".equalsIgnoreCase(metric))
			// return in ms rather than nano
			return Long.toString(global.getCacheStats(cs).totalLoadTime() / 1000000);

		if ("hit_rate".equalsIgnoreCase(metric))
			return Integer.toString((int) (global.getCacheStats(cs).hitRate() * 100));

		if ("miss_rate".equalsIgnoreCase(metric))
			return Integer.toString((int) (global.getCacheStats(cs).missRate() * 100));

		throw new PENotFoundException("Unable to parse status variable " + name + " - unknown metric " + metric);
	}

	@Override
	public void reset(CatalogDAO c, String name) throws PENotFoundException {
		SchemaCache global = SchemaSourceFactory.peekGlobalCache();

		if (global == null)
			return;

		// The format of defaultValue should be <Cache Segment>,<metric>
		int index = defaultValue.indexOf(',');

		if (index == -1)
			throw new PENotFoundException("Unable to parse status variable " + name + " defaultValue " + defaultValue);

		String segment = defaultValue.substring(0, index).trim();
		String metric = defaultValue.substring(index + 1).trim();

		CacheSegment cs = CacheSegment.valueOf(segment);

		if ("size".equalsIgnoreCase(metric) || "utilization".equalsIgnoreCase(metric)) {
			// Do nothing here - not about to flush the cache
			return;
		}

		if ("evictions".equalsIgnoreCase(metric) || "load_time".equalsIgnoreCase(metric)
				|| "hit_rate".equalsIgnoreCase(metric) || "miss_rate".equalsIgnoreCase(metric)) {
			global.resetCacheStats(cs);
		}
		else {
			throw new PENotFoundException("Unable to parse status variable " + name + " - unknown metric " + metric);
		}
	}
}

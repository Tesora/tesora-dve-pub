package com.tesora.dve.variable.status;

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

import com.tesora.dve.sql.schema.cache.CacheLimits;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaCache;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;

public class CacheStatusVariableHandler extends EnumStatusVariableHandler<CacheSegment> {

	private final CacheStatisticHandler handler;
	
	public CacheStatusVariableHandler(CacheSegment segment, CacheStatisticHandler handler) {
		super(buildVariableName(segment,handler),segment);
		this.handler = handler;
	}
	
	private static String buildVariableName(CacheSegment segment, CacheStatisticHandler handler) {
		StringBuilder buf = new StringBuilder();
		buf.append("Dve");
		if (!"".equals(segment.getStatusVariableSuffix()))
			buf.append("_").append(segment.getStatusVariableSuffix());
		buf.append("_cache_").append(handler.getExternalName());
		return buf.toString();
	}

	@Override
	protected long getCounterValue(CacheSegment counter) throws Throwable {
		SchemaCache global = SchemaSourceFactory.peekGlobalCache();
		CacheLimits limits = SchemaSourceFactory.peekCacheLimits();
		
		if (global == null)
			return 0;
		
		return handler.getValue(global, limits, counter);
	}

	@Override
	protected void resetCounterValue(CacheSegment counter) throws Throwable {
		SchemaCache global = SchemaSourceFactory.peekGlobalCache();

		if (global == null)
			return;

		handler.resetValue(global, counter);
	}
	
	public static abstract class CacheStatisticHandler {
		
		private final String externalName;
		
		public CacheStatisticHandler(String extName) {
			externalName = extName;
		}
		
		public String getExternalName() {
			return externalName;
		}
		
		public abstract long getValue(SchemaCache sc, CacheLimits cl, CacheSegment cs);
		
		public void resetValue(SchemaCache sc, CacheSegment cs) {
			sc.resetCacheStats(cs);			
		}
	}
	
	public static final CacheStatisticHandler sizeHandler = new CacheStatisticHandler("size") {
		
		@Override
		public long getValue(SchemaCache sc, CacheLimits cl, CacheSegment cs) {
			return sc.getCacheSize(cs);
		}

		@Override
		public void resetValue(SchemaCache sc, CacheSegment cs) {
		}
				
	};

	public static final CacheStatisticHandler utilizationHandler = new CacheStatisticHandler("utilization") {

		@Override
		public long getValue(SchemaCache sc, CacheLimits cl, CacheSegment cs) {
			int limit = cl.getLimit(cs);
			if (limit == 0) return 0;
			return sc.getCacheSize(cs) * 100 / limit;
		}

		@Override
		public void resetValue(SchemaCache sc, CacheSegment cs) {
			// TODO Auto-generated method stub
			
		}
		
	};

	public static final CacheStatisticHandler evictionHandler = new CacheStatisticHandler("evictions") {

		@Override
		public long getValue(SchemaCache sc, CacheLimits cl, CacheSegment cs) {
			return sc.getCacheStats(cs).evictionCount();
		}
		
	};
	
	public static final CacheStatisticHandler loadTimeHandler = new CacheStatisticHandler("load_time") {

		@Override
		public long getValue(SchemaCache sc, CacheLimits cl, CacheSegment cs) {
			return sc.getCacheStats(cs).totalLoadTime() / 1000000;
		}

	};
	
	public static final CacheStatisticHandler hitRateHandler = new CacheStatisticHandler("hit_rate") {

		@Override
		public long getValue(SchemaCache sc, CacheLimits cl, CacheSegment cs) {
			return (long)(sc.getCacheStats(cs).hitRate() * 100);
		}
		
	};
	
	public static final CacheStatisticHandler missRateHandler = new CacheStatisticHandler("miss_rate") {

		@Override
		public long getValue(SchemaCache sc, CacheLimits cl, CacheSegment cs) {
			return (long)(sc.getCacheStats(cs).missRate() * 100);
		}
		
	};
}

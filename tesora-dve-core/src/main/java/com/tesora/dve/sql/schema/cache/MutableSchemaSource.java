package com.tesora.dve.sql.schema.cache;

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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.cache.CacheStats;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.Functional;

public class MutableSchemaSource extends BasicSchemaSource implements SchemaSource {

	private final PlanCache planCache;
	
	private final PreparedStatementPlanCache pstmtCache;
	
	private final long versionAtCreation;
	private final boolean mutable;
	
	public MutableSchemaSource(long version, boolean mutable, PreparedStatementPlanCache pcache) {
		super(mutable ? CacheType.MUTABLE : CacheType.UNMUTABLE);
		versionAtCreation = version;
		this.mutable = mutable;
		this.pstmtCache = pcache;
		this.planCache = new MutablePlanCache();
	}

	@Override
	public long getVersion() {
		return versionAtCreation;
	}
	
	@Override
	public CacheType getType() {
		return (mutable ? CacheType.MUTABLE : CacheType.UNMUTABLE);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <T> SchemaEdge<T> buildEdge(T p) {
		Cacheable c = (Cacheable)p;
		SchemaEdge<T> e = (p == null ? new LoadedEdge() : new LoadedEdge(c.getCacheKey()));
		if (p != null) {
			tloaded.put(e.getCacheKey(),p);
		}
		return e;
	}

	@Override
	public <T> SchemaEdge<T> buildTransientEdge(T p) {
		return new TransientEdge<T>(p);
	}

	@Override
	public <T> SchemaEdge<T> buildEdgeFromKey(SchemaCacheKey<T> sck) {
		return new LoadedEdge<T>(sck);
	}

	
	@Override
	public boolean isPlanCacheEmpty() {
		if (mutable) return true;
		return planCache.isEmpty();
	}

	@Override
	public boolean canCachePlans(SchemaContext sc) {
		if (mutable) return false;
		return (SchemaSourceFactory.getCacheLimits(sc).getLimit(CacheSegment.PLAN) > 0);
	}
	
	@Override
	public void putCachedPlan(RegularCachedPlan cp) {
		planCache.put(cp);
	}

	@Override
	public RegularCachedPlan getCachedPlan(SchemaContext sc, PlanCacheKey pck) {
		return planCache.get(sc,pck);
	}

	@Override
	public void clearCachedPlan(RegularCachedPlan pck) {
		planCache.invalidate(pck.getKey());
	}

	@Override
	public void onCacheLimitUpdate(CacheSegment cs, CacheLimits limits) {
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("MutableSchemaSource@").append(System.identityHashCode(this))
			.append(", init version ").append(versionAtCreation).append(", mutable=").append(mutable).append(PEConstants.LINE_SEPARATOR);
		buf.append("by SchemaCacheKey {")
			.append(Functional.joinToString(tloaded.keySet(), ", ")).append("}");
		return buf.toString();
	}

	@Override
	public CachedPreparedStatement getPreparedStatement(PlanCacheKey pck) {
		return pstmtCache.getPreparedStatement(pck);
	}

	@Override
	public CachedPreparedStatement getPreparedStatement(SchemaContext sc, int connID, String stmtID) throws PEException {
		return pstmtCache.getPreparedStatement(sc,connID,stmtID);
	}

	@Override
	public void putPreparedStatement(CachedPreparedStatement cps, int connID, String stmtID, String rawSQL,	boolean reregister) throws PEException {
		pstmtCache.putPreparedStatement(cps, connID, stmtID, rawSQL, reregister);
	}

	@Override
	public void clearPreparedStatement(int connID, String stmtID) {
		pstmtCache.clearPreparedStatement(connID, stmtID);
	}

	@Override
	public void invalidate(SchemaCacheKey<?> sck) {
		pstmtCache.invalidate(sck);
	}

	private static class MutablePlanCache extends PlanCache {

		private final ConcurrentHashMap<PlanCacheKey, RegularCachedPlan> planCache = new ConcurrentHashMap<PlanCacheKey, RegularCachedPlan>();
		
		// in the mutable cache there is no limit to the size of the raw plan cache
		public MutablePlanCache() {
			super(-1);
		}
		
		@Override
		public boolean isMainEmpty() {
			return planCache.isEmpty();
		}

		@Override
		public long getMainSize() {
			return planCache.size();
		}

		@Override
		public CacheStats getMainStats() {
			return null;
		}

		@Override
		public void resetMainStats() {
		}

		@Override
		public void invalidateMain(PlanCacheKey pck) {
			planCache.remove(pck);
		}

		@Override
		public RegularCachedPlan getMain(PlanCacheKey pck) {
			return planCache.get(pck);
		}

		@Override
		public void putMain(RegularCachedPlan rcp) {
			planCache.put(rcp.getKey(),rcp);
		}

		@Override
		public Map<PlanCacheKey, RegularCachedPlan> getMainMap() {
			return planCache;
		}
		
	}
	
}

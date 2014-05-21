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




import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryProcedure;

// this is the immutable global cache
public class SchemaCache implements SchemaSource, RemovalListener<SchemaCacheKey<?>, Object> {

	private GlobalPlanCache planCache;
	
	private final Cache<SchemaCacheKey<?>,Object>[] tloaded;
	private final CacheStats[] tloaded_baseline;
	
	private final PreparedStatementPlanCache pstmtCache;
	
	private final long version;
	
	@SuppressWarnings("unchecked")
	public SchemaCache(long versionAtCreation, CacheLimits limits, PreparedStatementPlanCache thePstmtCache) {
		CacheSegment[] values = CacheSegment.values();

		tloaded = (Cache<SchemaCacheKey<?>, Object>[]) Array.newInstance(Cache/*<SchemaCacheKey<?>,Object>*/.class, values.length); 
		tloaded_baseline = new CacheStats[values.length];

		for(CacheSegment cs : values) {
			if (cs == CacheSegment.PLAN)
				continue;
			tloaded[cs.ordinal()] = buildCache(limits.getLimit(cs));
			tloaded_baseline[cs.ordinal()] = null;
		}
		planCache = new GlobalPlanCache(limits);
		pstmtCache = thePstmtCache;
		version = versionAtCreation;
	}
	
	private Cache<SchemaCacheKey<?>, Object> buildCache(int size) {
		CacheBuilder<SchemaCacheKey<?>, Object> boo = CacheBuilder.newBuilder().removalListener(this).recordStats();
		if (size > -1)
			boo = boo.maximumSize(size);
		return boo.build();
	}

	private static Cache<PlanCacheKey, RegularCachedPlan> buildPlanCache(int size) {
		return CacheBuilder.newBuilder().maximumSize(size).recordStats().build();		
	}
	
	private static class ContextLoader<T> implements Callable<T> {

		private final SchemaContext cntxt;
		private final SchemaCacheKey<T> key;
		
		public ContextLoader(SchemaContext csc, SchemaCacheKey<T> k) {
			cntxt = csc;
			key = k;
		}
		
		@Override
		public T call() throws Exception {
			cntxt.addCacheLoading(key);
			T candidate = key.load(cntxt);
			cntxt.removeCacheLoading(key);
			if (candidate == null)
				throw new PENotFoundException("unable to find " + key);
			return candidate;
		}
		
	}
	
	private static class ConstantLoader<T> implements Callable<T> {
		
		private final T target;
		
		public ConstantLoader(T t) {
			target = t;
		}

		@Override
		public T call() throws Exception {
			return target;
		}
	}
	
	public long getCacheSize(CacheSegment cs) {
		if (cs == CacheSegment.PLAN)
			return planCache.getSize();
		
		return getSubCache(cs).size();
	}
	
	public CacheStats getCacheStats(CacheSegment cs) {
		if (cs == CacheSegment.PLAN)
			return planCache.getStats();
		
		return getSubCacheStats(cs);
	}
	
	public void resetCacheStats(CacheSegment cs) {
		if (cs == CacheSegment.PLAN)
			planCache.resetStats();
		else
			tloaded_baseline[cs.ordinal()] = getSubCache(cs).stats();
	}

	private Cache<SchemaCacheKey<?>, Object> getSubCache(CacheSegment cs) {
		return tloaded[cs.ordinal()];
	}
	
	private CacheStats getSubCacheStats(CacheSegment cs) {
		if(tloaded_baseline[cs.ordinal()] != null)
			return tloaded[cs.ordinal()].stats().minus(tloaded_baseline[cs.ordinal()]);

		return tloaded[cs.ordinal()].stats();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void loadingGet(SchemaCacheKey<?> key, Object value) {
		try {
			getSubCache(key.getCacheSegment()).get(key, new ConstantLoader(value));
		} catch (ExecutionException ee) {
		}
	}
	

	@SuppressWarnings("unchecked")
	@Override
	public <T> T find(SchemaContext cntxt, SchemaCacheKey<T> ck) {
		if (ck == null) return null;
		try {
			return (T)getSubCache(ck.getCacheSegment()).get(ck, new ContextLoader<T>(cntxt,ck));
		} catch (ExecutionException ee) {
			if (ee.getCause() instanceof PENotFoundException)
				return null;
			throw new SchemaException(Pass.SECOND,ee.getCause());
		}
	}

	@Override
	public void setLoaded(final Persistable<?, ?> p, SchemaCacheKey<?> sck) {
		if (sck != null) {
			if (p == null) {
				getSubCache(sck.getCacheSegment()).invalidate(sck);
			} else {
				loadingGet(sck,p);
			}
		}
	}

	@Override
	public Persistable<?,?> getLoaded(SchemaCacheKey<?> sck) {
		if (sck == null) return null;
		return (Persistable<?, ?>) getSubCache(sck.getCacheSegment()).getIfPresent(sck);
	}

	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <T> SchemaEdge<T> buildEdge(T p) {
		Cacheable c = (Cacheable)p;
		SchemaEdge<T> e = (p == null ? new LoadedEdge() : new LoadedEdge(c.getCacheKey()));
		return e;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <T> SchemaEdge<T> buildTransientEdge(T p) {
		return new TransientEdge(p);
	}

	
	@Override
	public <T> SchemaEdge<T> buildEdgeFromKey(SchemaCacheKey<T> sck) {
		return new LoadedEdge<T>(sck);
	}
	
	@Override
	public CacheType getType() {
		return CacheType.GLOBAL;
	}
	
	@Override
	public long getVersion() {
		return version;
	}

	// used for debugging only
	public String describePlanCache() {
		StringBuffer buf = new StringBuffer();
		for(Map.Entry<PlanCacheKey, RegularCachedPlan> me : planCache.getROMap().entrySet()) {
			buf.append(me.getKey().toString()).append("==>").append(me.getValue().getClass().getSimpleName()).append(PEConstants.LINE_SEPARATOR);
		}
		return buf.toString();
	}
	
	// dump plan cache
	public void applyOnPlans(UnaryProcedure<RegularCachedPlan> f) {
		for(RegularCachedPlan rcp : planCache.getROMap().values())
			f.execute(rcp);
	}
	
	@Override
	public boolean isPlanCacheEmpty() {
		return planCache.isEmpty();
	}

	@Override
	public boolean canCachePlans(SchemaContext sc) {
		return SchemaSourceFactory.getCacheLimits(sc).getLimit(CacheSegment.PLAN) > 0;
	}
	
	@Override
	public void putCachedPlan(RegularCachedPlan cp) {
		planCache.put(cp);
	}

	@Override
	public void clearCachedPlan(RegularCachedPlan cp) {
		planCache.invalidate(cp.getKey());
	}
	
	@Override
	public RegularCachedPlan getCachedPlan(SchemaContext sc, PlanCacheKey pck) {
		return planCache.get(sc, pck);
	}

	@Override
	public void onCacheLimitUpdate(CacheSegment cs, CacheLimits limits) {
		if (cs == CacheSegment.PREPARED) return;
		if (cs == CacheSegment.PLAN || cs == CacheSegment.RAWPLAN) {
			planCache = new GlobalPlanCache(limits);
			if (cs == CacheSegment.RAWPLAN) {
				tloaded[cs.ordinal()] = buildCache(limits.getLimit(cs));				
				tloaded_baseline[cs.ordinal()] = null;
			}
		} else {
			tloaded[cs.ordinal()] = buildCache(limits.getLimit(cs));
			tloaded_baseline[cs.ordinal()] = null;
			if (cs == CacheSegment.TABLE || cs == CacheSegment.TENANT)
				planCache = new GlobalPlanCache(limits);
		}
	}
	
	@Override
	public void onRemoval(
			RemovalNotification<SchemaCacheKey<?>, Object> paramRemovalNotification) {
		SchemaCacheKey<?> sck = paramRemovalNotification.getKey();
		if (sck.getCacheSegment() != CacheSegment.UNCATEGORIZED) {
			HashSet<PlanCacheKey> current = new HashSet<PlanCacheKey>(planCache.getROMap().keySet());
			for(PlanCacheKey pck : current) {
				CachedPlan cp = planCache.get(null,pck);
				if (cp == null) continue;
				if (cp.invalidate(sck))
					planCache.invalidate(pck);
			}
			pstmtCache.invalidate(sck);
		}
	}

	// return true to invalidate the entire cache
	public boolean onModification(CacheInvalidationRecord cir) {
		if (cir.getGlobalToken() != null) return true;
		// if the key is null, it's something we don't bother caching
		if (cir.getInvalidateActions().isEmpty()) return false;
		// build the set of all stuff to invalidate
		HashSet<SchemaCacheKey<?>> visited = new HashSet<SchemaCacheKey<?>>();
		LinkedList<SchemaCacheKey<?>> toVisit = new LinkedList<SchemaCacheKey<?>>();
		ListSet<SchemaCacheKey<?>> toInvalidate = new ListSet<SchemaCacheKey<?>>();
		boolean haveRawPlanEvent = false;
		for(Pair<SchemaCacheKey<?>, InvalidationScope> p : cir.getInvalidateActions()) {
			if (p.getSecond() == InvalidationScope.GLOBAL)
				return true;
			if (p.getFirst().getCacheSegment() == CacheSegment.RAWPLAN)
				haveRawPlanEvent = true;
			else if (p.getSecond() == InvalidationScope.CASCADE) {
				toVisit.add(p.getFirst());
			}
			else
				toInvalidate.add(p.getFirst());
		}
		while(!toVisit.isEmpty()) {
			SchemaCacheKey<?> first = toVisit.removeFirst();
			if (first == null || !visited.add(first)) continue;
			toInvalidate.add(first);
			Cache<SchemaCacheKey<?>, Object> sub = getSubCache(first.getCacheSegment());
			Object present = sub.getIfPresent(first);
			if (present != null) 
				toVisit.addAll(first.getCascades(present));
		}
		// if we're removing a scope or a table - both are now contained in the cache aware lookups -
		// so just remove the entry for the associated object - and let the usual invalidation code handle
		// the plan cache.
		for(SchemaCacheKey<?> sck : toInvalidate) {
			if (sck.getCacheSegment() != CacheSegment.UNCATEGORIZED) {
				// make sure we invalidate both the forward and reverse lookups
				invalidate(sck);
			} else {
				return true;
			}
		}
		if (haveRawPlanEvent)
			planCache.onRawPlanEvent();
		return false;
	}

	@Override
	public void invalidate(SchemaCacheKey<?> sck) {
		Cache<SchemaCacheKey<?>, Object> c = getSubCache(sck.getCacheSegment());
		c.invalidate(sck);	
	}
	
	@Override
	public CachedPreparedStatement getPreparedStatement(PlanCacheKey pck) {
		return pstmtCache.getPreparedStatement(pck);
	}

	@Override
	public CachedPreparedStatement getPreparedStatement(SchemaContext sc, int connID, String stmtID) throws PEException {
		return pstmtCache.getPreparedStatement(sc, connID, stmtID);
	}

	@Override
	public void putPreparedStatement(CachedPreparedStatement cps, int connID, String stmtID, String rawSQL,
			boolean reregister) throws PEException {
		pstmtCache.putPreparedStatement(cps, connID, stmtID, rawSQL, reregister);
	}

	@Override
	public void clearPreparedStatement(int connID, String stmtID) {
		pstmtCache.clearPreparedStatement(connID, stmtID);
	}
	
	private static class GlobalPlanCache extends PlanCache {

		private final Cache<PlanCacheKey, RegularCachedPlan> planCache;
		private CacheStats baselineStats = null;

		public GlobalPlanCache(CacheLimits limits) {
			super(limits.getLimit(CacheSegment.RAWPLAN));
			planCache = buildPlanCache(limits.getLimit(CacheSegment.PLAN));
		}
		
		@Override
		public boolean isMainEmpty() {
			return planCache.size() == 0;
		}

		@Override
		public void invalidateMain(PlanCacheKey pck) {
			planCache.invalidate(pck);
		}

		@Override
		public RegularCachedPlan getMain(PlanCacheKey pck) {
			return planCache.getIfPresent(pck);
		}

		@Override
		public void putMain(RegularCachedPlan rcp) {
			planCache.put(rcp.getKey(),rcp);
		}

		@Override
		public long getMainSize() {
			return planCache.size();
		}

		@Override
		public CacheStats getMainStats() {
			if(baselineStats != null) 
				return planCache.stats().minus(baselineStats);

			return planCache.stats();
		}
		
		@Override
		public void resetMainStats() {
			baselineStats = planCache.stats();
		}

		@Override
		public Map<PlanCacheKey, RegularCachedPlan> getMainMap() {
			return planCache.asMap();
		}
		
		
	}
}

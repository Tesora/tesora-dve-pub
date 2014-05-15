// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.cache.CacheStats;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PERawPlan;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public abstract class PlanCache {

	public enum RawLoadState {
		// don't bother checking or else the raw plans are loaded
		LOADED,
		// next time we have a context handy, load raw plans
		LOAD_REQUIRED
		
	}
	
	private final ConcurrentHashMap<PlanCacheKey, RegularCachedPlan> rawPlans;
	private final int rawSize;
	private RawLoadState rawState;
	
	public PlanCache(int maxRaw) {
		rawPlans = new ConcurrentHashMap<PlanCacheKey, RegularCachedPlan>();
		rawSize = maxRaw;
		rawState = RawLoadState.LOAD_REQUIRED;
	}
		
	public final boolean isEmpty() {
		return isMainEmpty() && (rawSize == 0 || (rawPlans.isEmpty() && rawState == RawLoadState.LOADED));
	}
	
	public abstract boolean isMainEmpty();
	
	public final long getSize() {
		return getMainSize() + (rawSize == 0 ? 0 : rawPlans.size());
	}
	
	public abstract long getMainSize();
	
	public final CacheStats getStats() {
		return getMainStats();
	}
	
	public final void resetStats() {
		resetMainStats();
	}

	public abstract CacheStats getMainStats();
	public abstract void resetMainStats();
	
	public final void invalidate(PlanCacheKey pck) {
		invalidateMain(pck);
		if (rawSize > 0 && rawPlans.containsKey(pck))
			onRawPlanEvent();
	}
	
	public abstract void invalidateMain(PlanCacheKey pck);
	
	public final RegularCachedPlan get(SchemaContext sc, PlanCacheKey pck) {
		if (rawSize > 0) {
			if (rawState == RawLoadState.LOAD_REQUIRED)
				maybeLoadRawPlans(sc);
			RegularCachedPlan plan = rawPlans.get(pck);
			if (plan != null) return plan;
		}
		return getMain(pck);
	}
	
	public final void onRawPlanEvent() {
		if (rawSize == 0) return;
		synchronized(this) {
			rawState = RawLoadState.LOAD_REQUIRED;
		}
	}
	
	public abstract RegularCachedPlan getMain(PlanCacheKey pck);
	
	public final void put(RegularCachedPlan rcp) {
		putMain(rcp);
	}
	
	public abstract void putMain(RegularCachedPlan rcp);
	
	public abstract Map<PlanCacheKey, RegularCachedPlan> getMainMap();
	
	public Map<PlanCacheKey, RegularCachedPlan> getROMap() {
		if (rawSize == 0)
			return getMainMap();
		LinkedHashMap<PlanCacheKey, RegularCachedPlan> out = new LinkedHashMap<PlanCacheKey,RegularCachedPlan>();
		out.putAll(rawPlans);
		out.putAll(getMainMap());
		return out;
	}
	
	private void maybeLoadRawPlans(final SchemaContext sc) {
		if (sc == null || rawSize == 0) return;
		List<PERawPlan> plans = sc.findEnabledRawPlans();
		List<RegularCachedPlan> built = Functional.apply(plans, new UnaryFunction<RegularCachedPlan,PERawPlan>() {

			@Override
			public RegularCachedPlan evaluate(PERawPlan object) {
				return object.getPlan(sc);
			}
			
		});
		synchronized(rawPlans) {
			if (rawState == RawLoadState.LOADED) {
				// may not have to do anything unless the size of the raw plan cache is different than 
				// that of built
				if (built.size() == rawPlans.size())
					return;
			}
			rawPlans.clear();
			for(int i = 0; i < built.size() && i < rawSize; i++) {
				RegularCachedPlan rcp = built.get(i);
				rawPlans.put(rcp.getKey(),rcp);
			}
			rawState = RawLoadState.LOADED;
		}
    }
    
    public static void registerPreparedStatementPlan(SchemaContext sc, CachedPreparedStatement cps, String prepareSQL, int connid, String stmtID, boolean reregister) throws PEException {
    	sc.getSource().putPreparedStatement(cps, connid, stmtID, prepareSQL, reregister);
    }
    
    public static void destroyPreparedStatement(SchemaContext sc, String stmtID) {
    	sc.getSource().clearPreparedStatement(sc.getConnection().getConnectionId(),stmtID);
    }

    public static ExecutionPlan bindPreparedStatement(SchemaContext sc, String stmtID, List<?> params) throws PEException {
    	CachedPreparedStatement cps = sc.getSource().getPreparedStatement(sc, sc.getConnection().getConnectionId(), stmtID);    	
    	return cps.rebuildPlan(sc, params);
    }
    
}

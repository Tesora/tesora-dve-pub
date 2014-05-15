// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.PreparePlanningResult;
import com.tesora.dve.sql.schema.SchemaContext;

// prepared stmts are logically cached per connection, but for dml caching we cache per jvm and maintain
// a mapping from the connection specific id to a global id.
public class PreparedStatementPlanCache implements PreparedPlanCache {

	private final PStmtCache cache;
	
	private ConcurrentHashMap<String, PStmtMappingEntry> clientMapping;
	
	// once the client mapping becomes this big, we have to stop
	private int maxSize;
	
	public PreparedStatementPlanCache(CacheLimits cl) {
		maxSize = cl.getLimit(CacheSegment.PREPARED);
		cache = new PStmtCache();
		clientMapping = new ConcurrentHashMap<String, PStmtMappingEntry>();
	}

	@Override
	public void onCacheLimitUpdate(CacheSegment cs, CacheLimits limits) {
		if (cs != CacheSegment.PREPARED) return;
		int newSize = limits.getLimit(CacheSegment.PREPARED);
		if (newSize == 0) {
			// this means disable, so we can clear everything now
			maxSize = 0;
			clientMapping.clear();
			cache.clear();
		} else {
			// if newSize is smaller - everything in flight will continue but no new stmts can be allocated;
			// if larger - then we just grow upwards
			maxSize = newSize;
		}
	}
	
	@Override
	public CachedPreparedStatement getPreparedStatement(PlanCacheKey pck) {
		return cache.lookup(pck);
	}

	@Override
	public CachedPreparedStatement getPreparedStatement(SchemaContext sc, int connID, String stmtID) throws PEException {
		String ckey = buildConnectionStmtKey(connID,stmtID);
		PStmtMappingEntry psme = clientMapping.get(ckey);
		if (psme == null)
			throw new PEException("Internal error - no prepared statement found for " + ckey);
		if (psme.get() == null) {
			// entry invalidated, replan
			PreparePlanningResult ppr = InvokeParser.reprepareStatement(sc, psme.getPrepareSQL(), stmtID);
			psme.setCachedPreparedStatement(ppr.getCachedPlan());
		}
		return psme.get();
	}

	@Override
	public void putPreparedStatement(CachedPreparedStatement cps, int connID, String stmtID, String rawSQL, boolean reregister) throws PEException {
		// if not reregistering, do the size check first
		if (!reregister && (clientMapping.size() + 1) >= maxSize)
			throw new PEException("Max number of prepared stmts exceeded.");
		String key = buildConnectionStmtKey(connID,stmtID);
		PStmtMappingEntry psme = cache.put(cps, rawSQL);
		clientMapping.put(key,psme);
		if (psme.get() == null)
			psme.setCachedPreparedStatement(cps);
	}

	@Override
	public void clearPreparedStatement(int connID, String stmtID) {
		String connKey = buildConnectionStmtKey(connID, stmtID);
		PStmtMappingEntry psme = clientMapping.remove(connKey);
		if (psme == null) return;
		cache.remove(psme.getKey());
	}
	
	@Override
	public void invalidate(SchemaCacheKey<?> sck) {
		cache.invalidate(sck);
	}
	
	private static class PStmtMappingEntry {
		
		private PlanCacheKey pck;
		private CachedPreparedStatement stmt;
		private final String originalSQL;
		
		public PStmtMappingEntry(CachedPreparedStatement stmt, String origSQL) {
			this.pck = stmt.getKey();
			this.stmt = stmt;
			this.originalSQL = origSQL;
		}
		
		public PlanCacheKey getKey() {
			return pck;
		}
		
		public void invalidate(SchemaCacheKey<?> sck) {
			if (stmt == null) return;
			if (stmt.invalidate(sck)) 
				stmt = null;
		}
		
		public CachedPreparedStatement get() {
			return stmt;
		}
		
		public String getPrepareSQL() {
			return originalSQL;
		}

		// for reprepare
		void setCachedPreparedStatement(CachedPreparedStatement stmt) {
			this.stmt = stmt;
		}
	}

	private static String buildConnectionStmtKey(int connid, String stmtID) {
		StringBuilder buf = new StringBuilder();
		buf.append(connid).append("/").append(stmtID);
		return buf.toString();
	}

	private static class MappingEntry {
		
		private int refcount;
		private PStmtMappingEntry entry;
		
		public MappingEntry(PStmtMappingEntry psme) {
			entry = psme;
			refcount = 0;
		}
		
		public PStmtMappingEntry getEntry(boolean counting) {
			if (counting) synchronized(this) {
				refcount++;
			}
			return entry;
		}
		
		public synchronized void decrement() {
			refcount--;
		}
		
		public synchronized boolean isUnreferenced() {
			return refcount <= 0;
		}
	}
	
	private static class PStmtCache {
		
		private ConcurrentHashMap<PlanCacheKey,MappingEntry> cache;
		
		public PStmtCache() {
			clear();
		}
		
		public void clear() {
			cache = new ConcurrentHashMap<PlanCacheKey,MappingEntry>();
		}
		
		public CachedPreparedStatement lookup(PlanCacheKey pck) {
			MappingEntry me = cache.get(pck);
			if (me == null) return null;
			return me.getEntry(false).get();
		}
		
		public void invalidate(SchemaCacheKey<?> sck) {
			for(MappingEntry psme : cache.values())
				psme.getEntry(false).invalidate(sck);			
		}
		
		public PStmtMappingEntry put(CachedPreparedStatement cps, String rawSQL) {
			MappingEntry me = cache.get(cps.getKey());
			if (me == null) {
				MappingEntry nme = new MappingEntry(new PStmtMappingEntry(cps,rawSQL));
				synchronized(cache) {
					me = cache.get(cps.getKey());
					if (me == null) {
						cache.put(cps.getKey(),nme);
						me = nme;
					}
				}
			}
			return me.getEntry(true);
		}
		
		public void remove(PlanCacheKey pck) {
			MappingEntry me = cache.get(pck);
			if (me == null) return;
			me.decrement();
			if (me.isUnreferenced()) {
				synchronized(cache) {
					if (me.isUnreferenced()) {
						cache.remove(pck);
					}
				}
			}
		}
	}
	
	
	
}

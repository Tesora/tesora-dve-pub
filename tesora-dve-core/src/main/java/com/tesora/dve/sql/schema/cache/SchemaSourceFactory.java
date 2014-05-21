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

import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.sql.schema.ConnectionContext;
import com.tesora.dve.sql.schema.SchemaContext;

public final class SchemaSourceFactory {

	private static final SchemaSourceFactory instance = new SchemaSourceFactory();
	
	// the single cache
	private volatile SchemaCache global = null;
	private CacheLimits limits = null;
	
	private final AtomicLong version = new AtomicLong(1);
	
	// we cache prepared statements separately - in the case of a global cache clear what we do is clear the backing plans
	// but not the cache itself
	private PreparedStatementPlanCache pstmtCache = null;
	
	private SchemaSourceFactory() {
		
	}
	
	private void resetInternal() {
		global = null;
		limits = null;
	}
	
	public static void onModification(CacheInvalidationRecord cir) {
		if (cir == null) return;
		// the version is bumped no matter what
		instance.version.incrementAndGet();
		SchemaCache copy = instance.global;
		if (copy != null && copy.onModification(cir))
			instance.global = null;
	}
	
	public static long getSchemaVersion() {
		return instance.version.get();
	}

	
	public CacheLimits getCacheLimits(ConnectionContext cc) {
		if (limits == null) {
			CacheLimits temp = new CacheLimits(cc);
			limits = temp;
		}
		return limits;
	}
		
	private SchemaSource getGlobalCache(ConnectionContext cc, SchemaSource in) {
		CacheLimits cl = getCacheLimits(cc);
		int gcl = cl.getLimit(CacheSegment.UNCATEGORIZED);
		if (gcl == 0) {
			if (in == null) return getNewCache(cc);
			if (in.getType() == CacheType.GLOBAL || in.getVersion() < version.longValue()) return getNewCache(cc);
			return in;
		} else {
			return getGlobalCache(cc);
		}		
	}

	private SchemaSource getGlobalCache(ConnectionContext cc) {
		
		if(global != null) 
			return global;
		
		synchronized(this) {

			if (global == null) 
				global = new SchemaCache(version.longValue(),getCacheLimits(cc),getPreparedStatementCache(cc));
			
		}
		return global;		
	}
	
	private PreparedStatementPlanCache getPreparedStatementCache(ConnectionContext cc) {
		
		if (pstmtCache != null)
			return pstmtCache;

		synchronized(this) {
			if (pstmtCache == null)
				pstmtCache = new PreparedStatementPlanCache(getCacheLimits(cc));
		}
		return pstmtCache;
	}
	
	private SchemaSource getNewCache(ConnectionContext cc) {
		return new MutableSchemaSource(version.longValue(),false,getPreparedStatementCache(cc));		
	}
	
	public static SchemaSource getMutableSource() {
		return new MutableSchemaSource(instance.version.longValue(),true,instance.pstmtCache);
	}
	
	public static SchemaSource getSource(ConnectionContext cc, SchemaSource in) {
		return instance.getGlobalCache(cc,in);
	}
	
	// used in transient tests - clear the cached instance
	public static void reset() {
		instance.resetInternal();
		instance.version.set(0);
	}
	
	public static CacheLimits getCacheLimits(SchemaContext sc) {
		return instance.getCacheLimits(sc.getConnection());
	}
	
	public static SchemaCache peekGlobalCache() {
		return instance.global;
	}
	
	public static CacheLimits peekCacheLimits() {
		return instance.limits;
	}
	
	public static void setCacheSegmentLimit(CacheSegment cs, int limit) {
		if (instance.limits != null) {
			instance.limits = instance.limits.take(cs, limit);
			PreparedStatementPlanCache pstmt = instance.pstmtCache;
			if (pstmt != null)
				pstmt.onCacheLimitUpdate(cs, instance.limits);
			SchemaCache tmp = instance.global;
			if (tmp != null)
				tmp.onCacheLimitUpdate(cs, instance.limits);
		}
	}	
}

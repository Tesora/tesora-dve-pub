// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;

public interface PreparedPlanCache {

	// look up a prepared plan independent of connection
	public CachedPreparedStatement getPreparedStatement(PlanCacheKey pck);
	
	// look up a prepared plan by connection
	public CachedPreparedStatement getPreparedStatement(SchemaContext sc, int connID, String stmtID) throws PEException;
	
	public void putPreparedStatement(CachedPreparedStatement cps, int connID, String stmtID, String rawSQL, boolean reregister) throws PEException;
	
	public void clearPreparedStatement(int connID, String stmtID);

	public void invalidate(SchemaCacheKey<?> sck);
	
	void onCacheLimitUpdate(CacheSegment cs, CacheLimits limits);
	
}

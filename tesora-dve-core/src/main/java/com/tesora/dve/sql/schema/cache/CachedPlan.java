// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

// everything needed to recreate a query plan
public interface CachedPlan {

	public PlanCacheKey getKey();
	
	// return true if this plan must be unloaded because the specified object has been unloaded
	public boolean invalidate(SchemaCacheKey<?> unloaded);	
}

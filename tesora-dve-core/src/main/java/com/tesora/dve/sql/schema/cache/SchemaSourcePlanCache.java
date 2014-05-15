// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.sql.schema.SchemaContext;

public interface SchemaSourcePlanCache extends PreparedPlanCache {

	public boolean isPlanCacheEmpty();
	
	public boolean canCachePlans(SchemaContext sc);
	
	public void putCachedPlan(RegularCachedPlan cp);
	
	public void clearCachedPlan(RegularCachedPlan pck);
	
	// if context is null we're looking to invalidate a plan; otherwise we're looking to find one.
	public RegularCachedPlan getCachedPlan(SchemaContext sc, PlanCacheKey pck);	
}

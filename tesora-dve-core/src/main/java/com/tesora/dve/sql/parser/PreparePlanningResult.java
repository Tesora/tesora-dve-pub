// OS_STATUS: public
package com.tesora.dve.sql.parser;

import java.util.Collections;

import com.tesora.dve.sql.schema.cache.CachedPreparedStatement;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;

public class PreparePlanningResult extends PlanningResult {

	// we get the cache all ready to go - if something goes wrong we'll just toss it later
	private final CachedPreparedStatement cachedPlan;
	
	public PreparePlanningResult(ExecutionPlan prepareMetadata, CachedPreparedStatement actualPlan, String origSQL) {
		super(Collections.singletonList(prepareMetadata),null,origSQL);
		cachedPlan = actualPlan;
	}
	
	public CachedPreparedStatement getCachedPlan() {
		return cachedPlan;
	}
	
	public boolean isPrepared() {
		return true;
	}
	
	public String getPrepareSQL() {
		return getOriginalSQL();
	}	
}

// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import java.util.Locale;

import com.tesora.dve.variable.VariableAccessor;
import com.tesora.dve.variable.VariableScopeKind;

// we use a segmented cache, one where items of a particular type are stored in a subcache.
// this allows each subcache to thrash independently and build it's own hot set over time.
public enum CacheSegment {

	/*
	 * The uncategorized cache is everything not stored in a particular cache
	 * i.e. databases, persistent groups, persistent sites, users, privileges, etc.
	 */
	UNCATEGORIZED("cache_limit",500),
	/*
	 * Tenant scopes
	 */
	SCOPE("scope_cache_limit",10000),
	/*
	 * Tenants
	 */
	TENANT("tenant_cache_limit",500),
	/*
	 * Tables.  In multitenant mode this is the backing tables; in regular mode it's just tables.
	 */
	TABLE("table_cache_limit",400),
	/*
	 * Plans.  In multitenant mode this is on backing table plans; otherwise regular plans.
	 */
	PLAN("plan_cache_limit",400),
	/*
	 * Raw plans.  We use a separate limit so that they don't clutter up the general area.
	 */
	RAWPLAN("raw_plan_cache_limit",100),
	/* 
	 * Templates.  We use a separate segment so that modifying templates doesn't flush the cache.
	 * Modifying a template only effects create stmts anyways.
	 */
	TEMPLATE("template_cache_limit",100),
	/*
	 * Prepared statements.  This is the global max.  Prepared statements are valid by connection only.
	 */
	PREPARED("max_prepared_stmt_count",256);
	
	private final String configVariableName;
	private final int defaultValue;
	private final VariableAccessor accessor;
	
	private CacheSegment(String confVarName, int defaultValue) {
		configVariableName = confVarName;
		this.defaultValue = defaultValue;
		accessor = new VariableAccessor(VariableScopeKind.DVE, configVariableName);
	}
	
	public String getVariableName() {
		return configVariableName;
	}
	
	public VariableAccessor getAccessor() {
		return accessor;
	}
	
	public int getDefaultValue() {
		return defaultValue;
	}
	
	public static CacheSegment lookupSegment(String varname) {
		if (varname == null || "".equals(varname)) return null;
		String lc = varname.toLowerCase(Locale.ENGLISH);
		for(CacheSegment cs : values()) {
			if (cs.getVariableName().equals(lc))
				return cs;
		}
		return null;
	}	
}

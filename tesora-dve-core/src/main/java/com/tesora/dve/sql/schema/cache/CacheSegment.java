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

import java.util.Locale;

import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.variables.KnownVariables;

// we use a segmented cache, one where items of a particular type are stored in a subcache.
// this allows each subcache to thrash independently and build it's own hot set over time.
public enum CacheSegment {

	/*
	 * The uncategorized cache is everything not stored in a particular cache
	 * i.e. databases, persistent groups, persistent sites, users, privileges, etc.
	 */
	UNCATEGORIZED(KnownVariables.CACHE_LIMIT,"",true),
	/*
	 * Tenant scopes
	 */
	SCOPE(KnownVariables.SCOPE_CACHE_LIMIT,"scope",true),
	/*
	 * Tenants
	 */
	TENANT(KnownVariables.TENANT_CACHE_LIMIT,"tenant",true),
	/*
	 * Tables.  In multitenant mode this is the backing tables; in regular mode it's just tables.
	 */
	TABLE(KnownVariables.TABLE_CACHE_LIMIT,"table",true),
	/*
	 * Plans.  In multitenant mode this is on backing table plans; otherwise regular plans.
	 */
	PLAN(KnownVariables.PLAN_CACHE_LIMIT,"plan",false),
	/*
	 * Raw plans.  We use a separate limit so that they don't clutter up the general area.
	 */
	RAWPLAN(KnownVariables.RAW_PLAN_CACHE_LIMIT,"raw_plan",false),
	/* 
	 * Templates.  We use a separate segment so that modifying templates doesn't flush the cache.
	 * Modifying a template only effects create stmts anyways.
	 */
	TEMPLATE(KnownVariables.TEMPLATE_CACHE_LIMIT,"template",true),
	/*
	 * Prepared statements.  This is the global max.  Prepared statements are valid by connection only.
	 */
	PREPARED(KnownVariables.MAX_PREPARED_STMT_COUNT,"prepared_stmt",false),
	/*
	 * Runtime query statistics cache.  This is the global max - we will store no more than this amount
	 * in each server AS WELL AS in the catalog.
	 */
	QSTATS(KnownVariables.QSTAT_CACHE_LIMIT,"query_statistics",false);
	
	private final VariableHandler<Long> variable;
	private final String statusVariableSuffix;
	// due to the loading architecture, we are starting to overload the enum.  this is true
	// when this segment is part of the general cache.  some segments are specially cached.
	private final boolean generalCache;
		
	private CacheSegment(VariableHandler<Long> variable, String statusVariableSuffix, boolean general) {
		this.variable = variable;
		this.statusVariableSuffix = statusVariableSuffix;
		this.generalCache = general;
	}
	
	public String getVariableName() {
		return variable.getName();
	}
	
	public String getStatusVariableSuffix() {
		return statusVariableSuffix;
	}
	
	public VariableHandler<Long> getVariable() {
		return variable;
	}
	
	public boolean isGeneralCache() {
		return generalCache;
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

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
import com.tesora.dve.variables.Variables;

// we use a segmented cache, one where items of a particular type are stored in a subcache.
// this allows each subcache to thrash independently and build it's own hot set over time.
public enum CacheSegment {

	/*
	 * The uncategorized cache is everything not stored in a particular cache
	 * i.e. databases, persistent groups, persistent sites, users, privileges, etc.
	 */
	UNCATEGORIZED(Variables.CACHE_LIMIT),
	/*
	 * Tenant scopes
	 */
	SCOPE(Variables.SCOPE_CACHE_LIMIT),
	/*
	 * Tenants
	 */
	TENANT(Variables.TENANT_CACHE_LIMIT),
	/*
	 * Tables.  In multitenant mode this is the backing tables; in regular mode it's just tables.
	 */
	TABLE(Variables.TABLE_CACHE_LIMIT),
	/*
	 * Plans.  In multitenant mode this is on backing table plans; otherwise regular plans.
	 */
	PLAN(Variables.PLAN_CACHE_LIMIT),
	/*
	 * Raw plans.  We use a separate limit so that they don't clutter up the general area.
	 */
	RAWPLAN(Variables.RAW_PLAN_CACHE_LIMIT),
	/* 
	 * Templates.  We use a separate segment so that modifying templates doesn't flush the cache.
	 * Modifying a template only effects create stmts anyways.
	 */
	TEMPLATE(Variables.TEMPLATE_CACHE_LIMIT),
	/*
	 * Prepared statements.  This is the global max.  Prepared statements are valid by connection only.
	 */
	PREPARED(Variables.MAX_PREPARED_STMT_COUNT);
	
	private final VariableHandler<Long> variable;
		
	private CacheSegment(VariableHandler<Long> variable) {
		this.variable = variable;
	}
	
	public String getVariableName() {
		return variable.getName();
	}
	
	public int getDefaultValue() {
		return variable.getDefaultOnMissing().intValue();
	}
	
	public VariableHandler<Long> getVariable() {
		return variable;
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

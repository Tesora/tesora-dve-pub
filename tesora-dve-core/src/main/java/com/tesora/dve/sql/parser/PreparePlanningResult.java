package com.tesora.dve.sql.parser;

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

import java.util.Collections;

import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.cache.CachedPreparedStatement;
import com.tesora.dve.sql.transform.execution.RootExecutionPlan;

public class PreparePlanningResult extends PlanningResult {

	// we get the cache all ready to go - if something goes wrong we'll just toss it later
	private final CachedPreparedStatement cachedPlan;
	
	public PreparePlanningResult(RootExecutionPlan prepareMetadata, CachedPreparedStatement actualPlan, ConnectionValues boundValues, String origSQL) {
		super(Collections.singletonList(prepareMetadata),boundValues,null,origSQL);
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

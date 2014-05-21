// OS_STATUS: public
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

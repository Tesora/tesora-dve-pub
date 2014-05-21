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



import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;

public interface SchemaSource extends SchemaSourcePlanCache {

	<T> T find(SchemaContext cntxt, SchemaCacheKey<T> ck);
	
	// schema context only calls this once the object is fully loaded upto any scope or table related
	// lazy lookup
	void setLoaded(Persistable<?,?> p, SchemaCacheKey<?> sck);
	
	// must not do the lookup
	Persistable<?,?> getLoaded(SchemaCacheKey<?> sck);
	
	// a regular edge is derived from persistent state - there's an existing relationship in the
	// catalog.  if the object isn't already mapped in the cache, add it.
	@SuppressWarnings("rawtypes")
	<T> SchemaEdge buildEdge(T p);	
	
	<T> SchemaEdge<T> buildTransientEdge(T p);
	
	// build an edge on the assumption that the target cache key is already loaded
	<T> SchemaEdge<T> buildEdgeFromKey(SchemaCacheKey<T> sck);

	// the version at creation time - used to detect when we should clear the whole cache
	long getVersion();
	
	public CacheType getType();
	
}

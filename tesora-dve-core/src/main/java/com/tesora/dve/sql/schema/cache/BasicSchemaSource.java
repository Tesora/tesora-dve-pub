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

import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;

public class BasicSchemaSource {

	protected final ConcurrentHashMap<SchemaCacheKey<?>, Object> tloaded = new ConcurrentHashMap<SchemaCacheKey<?>, Object>();

	protected final CacheType type;
	
	public BasicSchemaSource(CacheType ct) {
		type = ct;
	}
	
	public CacheType getType() {
		return type;
	}
	
	public void clear() {
		tloaded.clear();
	}
	
	public <T> T find(SchemaContext cntxt, SchemaCacheKey<T> ck) {
		if (ck == null) return null;
		@SuppressWarnings("unchecked")
		T candidate = (T) tloaded.get(ck);
		if (candidate == null) {
			candidate = ck.load(cntxt);
			if (candidate == null) return null;
			tloaded.put(ck, candidate);
		}
		return candidate;
	}
		
	// schema context only calls this once the object is fully loaded upto any scope or table related
	// lazy lookup
	public void setLoaded(Persistable<?,?> p, SchemaCacheKey<?> sck) {
		if (sck != null) {
			if (p == null) { 
				tloaded.remove(sck);
			} else {
				tloaded.put(sck, p);
			}
		}
	}

	public Persistable<?,?> getLoaded(SchemaCacheKey<?> sck) {
		if (sck == null) return null;
		return (Persistable<?, ?>) tloaded.get(sck);
	}
	
}

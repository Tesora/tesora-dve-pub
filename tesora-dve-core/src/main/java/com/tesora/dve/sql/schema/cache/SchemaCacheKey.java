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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.SchemaContext;

public abstract class SchemaCacheKey<T> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	protected SchemaCacheKey() {
	}
	
	@Override
	public abstract int hashCode();	
	@Override
	public abstract boolean equals(Object o);	
	public abstract T load(SchemaContext sc);
	@Override
	public abstract String toString();
	
	public CacheSegment getCacheSegment() {
		return CacheSegment.UNCATEGORIZED;
	}
		
	public Collection<SchemaCacheKey<?>> getCascades(Object obj) {
		return Collections.emptySet();
	}
	
	public SchemaCacheKey<?> getEnclosing() {
		return null;
	}
	
	protected int addHash(int result, int hc) {
		final int prime = 31;
		return prime * result + hc;
	}

	protected int addIntHash(int result, int value) {
		return addHash(result, value ^ (value >>> 32));
	}
	
	protected int initHash(Class<?> c, int hc) {
		return addHash(c.hashCode(),hc);
	}
	
	public LockSpecification getLockSpecification(String reason) {
		return null;
	}
	
	public void acquireLock(SchemaContext sc, LockInfo info) {
		if (info == null) return;
		LockSpecification ls = getLockSpecification(info.getReason());
		if (ls == null) return;
		sc.getConnection().acquireLock(ls, info.getType());
	}
}

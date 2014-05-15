// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

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

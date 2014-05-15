// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

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

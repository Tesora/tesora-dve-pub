// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.sql.schema.SchemaContext;

class LoadedEdge<TClass> implements SchemaEdge<TClass> {
	private SchemaCacheKey<TClass> key;
	
	public LoadedEdge() {
		key = null;
	}
	
	public LoadedEdge(SchemaCacheKey<TClass> k) {
		key = k;
	}
	
	@Override
	public TClass get(SchemaContext sc) {
		if (key == null) return null;
		return sc.getSource().find(sc, key);
	}

	@Override
	public SchemaCacheKey<TClass> getCacheKey() {
		return key;
	}

	@Override
	public boolean has() {
		return key != null;
	}	
}
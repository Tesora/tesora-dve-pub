// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.sql.schema.SchemaContext;

public class TransientEdge<TClass> implements SchemaEdge<TClass> {
	private TClass target;
	
	public TransientEdge(TClass t) {
		target = t;
	}

	@Override
	public TClass get(SchemaContext sc) {
		return target;
	}

	@Override
	public SchemaCacheKey<TClass> getCacheKey() {
		return null;
	}

	@Override
	public boolean has() {
		return target != null;
	}	
}
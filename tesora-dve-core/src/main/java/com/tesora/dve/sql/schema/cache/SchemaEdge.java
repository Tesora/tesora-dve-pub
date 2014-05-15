// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import com.tesora.dve.sql.schema.SchemaContext;

public interface SchemaEdge<TClass> {

	public TClass get(SchemaContext sc);
	
	public SchemaCacheKey<TClass> getCacheKey();
	
	// return true if the edge has a value (i.e. the key is not null)
	public boolean has();		
}

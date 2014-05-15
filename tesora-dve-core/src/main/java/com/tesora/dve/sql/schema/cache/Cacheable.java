// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

public interface Cacheable<T> {

	public SchemaCacheKey<T> getCacheKey();
	
}

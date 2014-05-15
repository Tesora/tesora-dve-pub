// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

public interface EntityCacheKey {

	@Override
	public int hashCode();

	@Override
	public boolean equals(Object obj);
	
}

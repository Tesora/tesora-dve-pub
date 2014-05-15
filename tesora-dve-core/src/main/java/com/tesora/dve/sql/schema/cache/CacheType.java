// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

public enum CacheType {

	// the global cache
	GLOBAL,
	// a mutable local cache
	MUTABLE,
	// an unmutable local cache - used when the global cache is turned off
	UNMUTABLE;
	
}

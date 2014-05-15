// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

// when invalidating the cache for a particular key, what other keys should we also invalidate
public enum InvalidationScope {

	// a global invalidation tosses the whole cache
	GLOBAL,
	// a local invalidation tosses just the key
	LOCAL,
	// a cascading invalidation tosses the key plus anything enclosed by it
	// so for a database it would toss the tables, for a tenant it tosses the tables, etc.
	CASCADE
}

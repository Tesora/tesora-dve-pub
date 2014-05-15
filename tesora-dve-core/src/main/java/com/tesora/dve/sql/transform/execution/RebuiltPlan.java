// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;

public class RebuiltPlan {
	protected ExecutionPlan ep;
	protected boolean clearCache = true;
	protected final SchemaCacheKey<?>[] cacheKeys;
	protected final LockType lockType;
	
	public RebuiltPlan(ExecutionPlan ep, boolean clearCache, SchemaCacheKey<?>[] keys, LockType lt) {
		this.ep = ep;
		this.clearCache = clearCache;
		this.cacheKeys = keys;
		this.lockType = lt;
	}

	public ExecutionPlan getEp() {
		return ep;
	}

	public boolean isClearCache() {
		return clearCache;
	}
	
	public SchemaCacheKey<?>[] getCacheKeys() {
		return cacheKeys;
	}
	
	public LockType getLockType() {
		return lockType;
	}
}

// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.mt.PETenant;

public class PEAlterTenantStatement extends PEAlterStatement<PETenant> {

	boolean suspended;
	
	public PEAlterTenantStatement(PETenant target, boolean suspend) {
		super(target, true);
		suspended = suspend;
	}

	public boolean isSuspend() {
		return suspended;
	}
	
	@Override
	protected PETenant modify(SchemaContext sc, PETenant backing) throws PEException {
		backing.setSuspended(suspended);
		return backing;
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		// local invalidation
		return new CacheInvalidationRecord(getTarget().getCacheKey(),InvalidationScope.LOCAL);
	}

}

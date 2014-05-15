// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import com.tesora.dve.common.catalog.User;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;

public class PEDropUserStatement extends
		PEDropStatement<PEUser, User> {

	public PEDropUserStatement(PEUser user) {
		super(PEUser.class, null, false, user, "USER");
	}
	
	@Override
	protected ExecutionStep buildStep(SchemaContext pc) throws PEException {
		// override to reset the persistent group
		return new SimpleDDLExecutionStep(null, buildAllSitesGroup(pc), getRoot(), getAction(), getSQLCommand(pc),
				getDeleteObjects(pc), getCatalogObjects(pc),new CacheInvalidationRecord(getTarget().getCacheKey(), InvalidationScope.CASCADE));
	}

}

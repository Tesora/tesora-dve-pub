// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;

public class SetPasswordStatement extends PEAlterStatement<PEUser> {

	private String newPassword;
	
	public SetPasswordStatement(PEUser target, String newpass) {
		super(target, false);
		newPassword = newpass;
	}

	public String getNewPassword() {
		return newPassword;
	}
	
	@Override
	protected PEUser modify(SchemaContext sc, PEUser backing) throws PEException {
		backing.setPassword(newPassword);
		return backing;
	}

	@Override
	protected ExecutionStep buildStep(SchemaContext pc) throws PEException {
		return new SimpleDDLExecutionStep(null, buildAllSitesGroup(pc), getRoot(), getAction(), getSQLCommand(pc),
				getDeleteObjects(pc), getCatalogObjects(pc), getInvalidationRecord(pc));
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext pc) {
		return new CacheInvalidationRecord(getTarget().getCacheKey(),InvalidationScope.LOCAL);
	}

	
}

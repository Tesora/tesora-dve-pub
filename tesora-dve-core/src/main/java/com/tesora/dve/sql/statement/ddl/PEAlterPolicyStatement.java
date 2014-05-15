// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEPolicy;
import com.tesora.dve.sql.schema.PEPolicyClassConfig;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;

public class PEAlterPolicyStatement extends PEAlterStatement<PEPolicy> {

	protected List<PEPolicyClassConfig> newConfig;
	protected Boolean newStrict;
	
	public PEAlterPolicyStatement(PEPolicy target, Name newName, Boolean newStrict, List<PEPolicyClassConfig> newConfig) {
		super(target, true);
		this.newConfig = newConfig;
		this.newStrict = newStrict;
	}

	@Override
	protected PEPolicy modify(SchemaContext sc, PEPolicy backing) throws PEException {
		backing.replacePolicy(newConfig);
		if (newStrict != null)
			backing.setStrict(newStrict.booleanValue());
		return backing;
	}

	public String getSQL() {
		return "";
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext pc) {
		return CacheInvalidationRecord.GLOBAL;
	}
	
}

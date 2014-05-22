package com.tesora.dve.sql.statement.ddl;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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

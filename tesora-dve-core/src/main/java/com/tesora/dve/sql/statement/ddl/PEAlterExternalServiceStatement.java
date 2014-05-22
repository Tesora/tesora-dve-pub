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
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEExternalService;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExternalServiceExecutionStep;
import com.tesora.dve.sql.util.Pair;

public class PEAlterExternalServiceStatement extends
		PEAlterStatement<PEExternalService> {
	List<Pair<Name,LiteralExpression>> newOptions = null;
	
	// this constructor takes a target with the new options already set
	public PEAlterExternalServiceStatement(PEExternalService target) {
		super(target, true);
	}

	public PEAlterExternalServiceStatement(PEExternalService target, List<Pair<Name,LiteralExpression>> newOptions) {
		super(target, true);
		
		this.newOptions = newOptions;
	}

	@Override
	protected PEExternalService modify(SchemaContext pc, PEExternalService backing)
			throws PEException {
		backing.parseOptions(pc, newOptions);
		return backing;
	}
	
	@Override
	protected ExecutionStep buildStep(SchemaContext pc) throws PEException {
		return new ExternalServiceExecutionStep(getDatabase(pc),
				getStorageGroup(pc), getRoot(), getAction(), getSQLCommand(pc),
				getDeleteObjects(pc), getCatalogObjects(pc), getInvalidationRecord(pc));
	}
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return CacheInvalidationRecord.GLOBAL;
	}

}

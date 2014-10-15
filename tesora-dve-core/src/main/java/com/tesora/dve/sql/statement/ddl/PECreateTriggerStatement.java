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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserTrigger;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.PETrigger;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;
import com.tesora.dve.sql.util.Functional;

public class PECreateTriggerStatement extends
		PECreateStatement<PETrigger, UserTrigger> {

	public PECreateTriggerStatement(Persistable<PETrigger, UserTrigger> targ) {
		super(targ, false, "TRIGGER", false);
	}

	// override because we don't push down the create trigger
	@Override
	protected CatalogModificationExecutionStep buildStep(SchemaContext pc) throws PEException {
		// go ahead and add this trigger to the table
		getCreated().get().getTargetTable(pc).addTrigger(pc, getCreated().get());
		return new SimpleDDLExecutionStep(getDatabase(pc), getStorageGroup(pc), getRoot(), getAction(), SQLCommand.EMPTY,
				getDeleteObjects(pc), getCatalogObjects(pc), getInvalidationRecord(pc));
	}

	// the actual root of the entities is the table
	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext pc) throws PEException {
		pc.beginSaveContext();
		try {
			getCreated().get().getTargetTable(pc).persistTree(pc);
			return Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}

	}
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return new CacheInvalidationRecord(getCreated().get().getTargetTable(sc).getCacheKey(),InvalidationScope.CASCADE);
	}

}

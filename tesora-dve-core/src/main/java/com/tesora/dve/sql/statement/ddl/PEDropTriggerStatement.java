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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserTrigger;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETrigger;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;

public class PEDropTriggerStatement extends PEDropStatement<PETrigger, UserTrigger> {

	private final PETable parent;
	private final PETrigger trigger;

	public PEDropTriggerStatement(final PETrigger trigger, final Boolean ifExists) {
		super(PETrigger.class, ifExists, false, trigger, "TRIGGER");
		this.parent = trigger.getTargetTable();
		this.trigger = trigger;
	}

	// override because we don't push down the create trigger
	@Override
	protected CatalogModificationExecutionStep buildStep(SchemaContext pc) throws PEException {
		// remove from the target table
		this.parent.removeTrigger(pc, this.trigger);
		return new SimpleDDLExecutionStep(getDatabase(pc), getStorageGroup(pc), getRoot(), getAction(), SQLCommand.EMPTY,
				getDeleteObjects(pc), getCatalogObjects(pc), getInvalidationRecord(pc));
	}

	// the actual root of the entities is the table
	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext pc) throws PEException {
		pc.beginSaveContext();
		try {
			this.parent.persistTree(pc); // persist the updated parent
			return Collections.<CatalogEntity> singletonList((UserTrigger) getRoot().getPersistent(pc));
		} finally {
			pc.endSaveContext();
		}

	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return new CacheInvalidationRecord(this.parent.getCacheKey(), InvalidationScope.CASCADE);
	}

}

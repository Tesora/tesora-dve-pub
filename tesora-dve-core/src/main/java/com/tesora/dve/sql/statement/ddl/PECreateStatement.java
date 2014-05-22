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
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.Functional;

// pe create statements may have an additional impact on the catalog
public class PECreateStatement<TransientClass, PersistentClass> extends DDLStatement {

	protected Persistable<TransientClass, PersistentClass> backing = null;
	protected Boolean ifNotExists;
	protected String tag;
	protected boolean alreadyExists;
	
	public PECreateStatement(Persistable<TransientClass, PersistentClass> targ, boolean dveOnly, String tag, boolean exists) {
		this(targ, dveOnly, null, tag, exists);
	}
	
	public PECreateStatement(Persistable<TransientClass, PersistentClass> targ, boolean dveOnly, Boolean ine, String specTag, boolean exists) {
		super(dveOnly);
		backing = targ;
		ifNotExists = ine;
		tag = specTag;
		alreadyExists = exists;
	}
		
	public boolean isNew() {
		return !alreadyExists;
	}
	
	public void setOld() {
		alreadyExists = true;
	}
	
	public Boolean isIfNotExists() {
		return ifNotExists;
	}
	
	public String getSchemaTag() {
		return tag;
	}
	
	public Persistable<TransientClass, PersistentClass> getCreated() {
		return backing;
	}
	
	public List<CatalogEntity> createCatalogObjects(SchemaContext pc) throws PEException {
		pc.beginSaveContext();
		try {
			getCreated().persistTree(pc);
			return Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}
	}

	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext pc) throws PEException {
		return createCatalogObjects(pc);
	}

	@Override	
	public PEStorageGroup getStorageGroup(SchemaContext pc) {
		if (getRoot() instanceof PETable)
			return ((PETable)getRoot()).getStorageGroup(pc);
		if (getRoot() instanceof PEDatabase)
			return ((PEDatabase)getRoot()).getDefaultStorage(pc);
		return super.getStorageGroup(pc);
	}

	@Override
	public Action getAction() {
		return Action.CREATE;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		return getCreated();
	}

	@Override
	protected CatalogModificationExecutionStep buildStep(SchemaContext pc) throws PEException {
		if (!alreadyExists)
			return new SimpleDDLExecutionStep(getDatabase(pc), getStorageGroup(pc), getRoot(), getAction(), getSQLCommand(pc),
					getDeleteObjects(pc), getCatalogObjects(pc), getInvalidationRecord(pc));
		else
			return null;
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return null;
	}

	
}

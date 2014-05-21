// OS_STATUS: public
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
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.Functional;

public abstract class PEAlterStatement<S extends Persistable<?,?>> extends AlterStatement {

	protected S backing;
	
	public PEAlterStatement(S target, boolean peOnly) {
		super(peOnly);
		backing = target;
	}

	public S getTarget() {
		return backing;
	}
	
	public void setTarget(S nt) {
		backing = nt;
	}
	
	protected abstract S modify(SchemaContext pc, S backing) throws PEException;

	public List<CatalogEntity> getCatalogEntries(SchemaContext pc) throws PEException {
		pc.beginSaveContext(true);
		try {
			backing.persistTree(pc,true);
			backing = modify(pc,backing);
		} finally {
			pc.endSaveContext();
		}
		pc.beginSaveContext(true);
		try {
			backing.persistTree(pc);
			return Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}
	}
		
	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext pc) throws PEException {
		return getCatalogEntries(pc);
	}

	@Override
	public Action getAction() {
		return Action.ALTER;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		return getTarget();
	}

}

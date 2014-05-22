package com.tesora.dve.sql.statement.ddl.alter;

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
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.ddl.PEAlterStatement;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;

public abstract class AlterTableAction {

	public static abstract class ClonableAlterTableAction extends AlterTableAction {

		public AlterTableAction makeTransientOnlyClone() {
			final AlterTableAction copy = clone();
			return copy.markTransientOnly();
		}

		protected abstract AlterTableAction clone();
	}

	private boolean transientOnly = false;

	// if this action has cascading actions, return this action and the cascading action (in the right order)
	// if this is not the case, return null.
	public abstract AlterTableAction alterTable(SchemaContext sc, PETable tab);
	
	// return true if the action specified by this alter must have already
	// taken place - in that case the alter is a noop - used in adaptive mt
	public abstract boolean isNoop(SchemaContext sc, PETable tab);
	
	// return a string describing why this alter is no longer valid, or null if it still is
	public abstract String isValid(SchemaContext sc, PETable tab);

	public abstract AlterTableAction adapt(SchemaContext sc, PETable actual);

	public void refresh(SchemaContext sc, PETable pet) {
		// default does nothing
	}
	
	@SuppressWarnings("unchecked")
	public List<CatalogEntity> getDeleteObjects(SchemaContext sc) throws PEException {
		return Collections.EMPTY_LIST;
	}

	public abstract AlterTargetKind getTargetKind();
	
	public abstract CatalogModificationExecutionStep.Action getActionKind();
	
	public boolean isPassthrough() {
		return false;
	}
	
	// generally true
	public boolean hasSQL(SchemaContext sc, PETable pet) {
		return true;
	}
	
	public PEAlterStatement<PETable> requiresSingleStatement(SchemaContext sc, TableKey target) {
		return null;
	}
	
    // actions aren't created knowing who the target is - allow that to be set so that downstream
    // methods can work correctly (i.e. emitter)
    public void setTarget(SchemaContext sc, PETable tab) {
      
    }

	public boolean isTransientOnly() {
		return this.transientOnly;
	}

	public AlterTableAction markTransientOnly() {
		this.transientOnly = true;
		return this;
	}

}

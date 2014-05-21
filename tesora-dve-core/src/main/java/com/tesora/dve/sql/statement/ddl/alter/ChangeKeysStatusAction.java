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

import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class ChangeKeysStatusAction extends AlterTableAction {

	private final boolean enable;
	
	public ChangeKeysStatusAction(boolean en) {
		super();
		enable = en;
	}
	
	public boolean isEnable() {
		return enable;
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		// does nothing
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		return false;
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		return null;
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		return new ChangeKeysStatusAction(enable);
	}

	@Override
	public AlterTargetKind getTargetKind() {
		return AlterTargetKind.TABLE;
	}

	@Override
	public Action getActionKind() {
		return Action.ALTER;
	}

	@Override
	public boolean isPassthrough() {
		return true;
	}

	
}

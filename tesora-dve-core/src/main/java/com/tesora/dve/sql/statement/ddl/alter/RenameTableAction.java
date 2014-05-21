// OS_STATUS: public
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

import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class RenameTableAction extends AlterTableAction {

	protected Name newName;
	protected boolean existenceCheck;
	
	public RenameTableAction(Name nn) {
		this(nn,true);
	}
	
	public RenameTableAction(Name nn, boolean doExistenceChecks) {
		newName = nn;
		existenceCheck = doExistenceChecks;
	}
	
	public Name getNewName() {
		return newName;
	}

	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		// make sure the new name is not in the same database as the old name
		PEDatabase ofdb = tab.getPEDatabase(sc);
		if (existenceCheck) {
			TableInstance uhoh = ofdb.getSchema().buildInstance(sc, newName.getUnqualified(),new LockInfo(LockType.EXCLUSIVE,"rename table"));
			if (uhoh != null)
				throw new SchemaException(Pass.PLANNER,"Table " + newName + " already exists");
		}
		tab.setName(newName);
		return null;
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		throw new SchemaException(Pass.PLANNER,"Invalid planning on rename table");
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public AlterTargetKind getTargetKind() {
		return AlterTargetKind.TABLE;
	}

	@Override
	public Action getActionKind() {
		return Action.ALTER;
	}
	
	
}

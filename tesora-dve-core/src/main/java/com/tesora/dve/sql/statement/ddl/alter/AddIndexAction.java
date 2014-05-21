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


import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class AddIndexAction extends AlterTableAction {

	private PEKey index;

	public AddIndexAction(PEKey ind) {
		index = ind;
	}
	
	public PEKey getNewIndex() {
		return index;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void setTarget(SchemaContext sc, PETable targ) {
		index.setTable(StructuralUtils.buildEdge(sc, targ, false));
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		PEKey copy = index.copy(sc, tab);
		PEKey invalidated = tab.addKey(sc, copy, true);
		if (invalidated != null) 			
			return new DropIndexAction(invalidated);
		return null;
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		if (index.getName() == null) return null;
		PEKey already = index.getIn(sc,tab);
		if (already != null)
			return "Table " + tab.getName() + " already has " + already.toString(); 
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		return index.getIn(sc, tab) != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		PEKey theKey = index.copy(sc, actual);
		theKey.setTable(StructuralUtils.buildEdge(sc,  actual,  false));
		return new AddIndexAction(theKey);
	}

	@Override
	public AlterTargetKind getTargetKind() {
		return AlterTargetKind.INDEX;
	}

	@Override
	public Action getActionKind() {
		return Action.CREATE;
	}

}

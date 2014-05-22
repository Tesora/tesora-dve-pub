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
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class DropColumnAction extends AbstractAlterColumnAction {

	protected PEColumn column;
	
	public DropColumnAction(PEColumn toDrop) {
		column = toDrop;
	}

	@Override
	public void refresh(SchemaContext sc, PETable pet) {
		super.refresh(sc, pet);
		column = (PEColumn) column.getIn(sc,pet);
	}
	
	public PEColumn getDroppedColumn() {
		return column;
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		PEColumn c = (PEColumn) column.getIn(sc,tab);
		tab.removeColumn(sc,c);
		return null;
	}

	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext sc) throws PEException {
		return Collections.singletonList((CatalogEntity)column.getPersistent(sc));
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		return new DropColumnAction(column);
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		PEColumn c = (PEColumn) column.getIn(sc,tab);
		if (c == null)
			return "Cannot drop nonexistent column " + column.getName() + " in table " + tab.getName();
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		return column.getIn(sc,tab) == null;
	}

	@Override
	public Action getActionKind() {
		return Action.DROP;
	}

	@Override
	public List<PEColumn> getColumns() {
		return Collections.singletonList(column);
	}
}

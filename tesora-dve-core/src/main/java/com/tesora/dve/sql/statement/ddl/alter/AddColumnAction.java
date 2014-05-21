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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

public class AddColumnAction extends AbstractAlterColumnAction {

	private List<PEColumn> newColumns;
	private Pair<String, Name> firstOrAfterSpec;
	
	public AddColumnAction(List<PEColumn> columns) {
		this(columns, null);
	}

	public AddColumnAction(List<PEColumn> columns, Pair<String, Name> firstOrAfterSpec) {
		newColumns = columns;
		for(PEColumn p : newColumns)
			p.normalize();
		this.firstOrAfterSpec = firstOrAfterSpec;
	}
	
	public List<PEColumn> getNewColumns() {
		return newColumns;
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		List<PEColumn> movedCols = null;
		if (firstOrAfterSpec != null) {
			// take a first pass and update the ordinal numbers
			int ordinal = -1;
			if (StringUtils.equalsIgnoreCase(firstOrAfterSpec.getFirst(), "AFTER")) {
				PEColumn afterColumn = tab.lookup(sc, firstOrAfterSpec.getSecond().getUnqualified().getUnquotedName().get());
				ordinal = afterColumn.getPosition();
			}
			movedCols = new ArrayList<PEColumn>();
			List<PEColumn> origCols = tab.getColumns(sc);
			for(PEColumn col : origCols) {
				if (col.getPosition() > ordinal) {
					movedCols.add(col);
				}
			}

			for(PEColumn col : movedCols) {
				tab.removeColumn(sc, col);
			}
		}
		for(PEColumn nc : newColumns) {
			if (nc.getIn(sc, tab) == null) {
				PEColumn copy = (PEColumn) nc.copy(sc, null);
				tab.addColumn(sc, copy);
				// add back the columns if FIRST or AFTER was specified
				if (movedCols != null) {
					for(PEColumn c : movedCols) {
						tab.addColumn(sc, c);
					}
				}
			}
		}
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		// only a noop if all are a noop
		for(PEColumn nc : newColumns) {
			if (nc.getIn(sc, tab) == null) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		if (firstOrAfterSpec != null && firstOrAfterSpec.getSecond() != null) {
			// validate the after column exists
			if (tab.lookup(sc, firstOrAfterSpec.getSecond()) == null) {
				return "Table " + tab.getName() + " does not contain column " + firstOrAfterSpec.getSecond().getSQL() + " that was specified with AFTER.  Cannot add.";
			}
		}
		// only valid if any of the columns doesn't already exist
		for(PEColumn nc : newColumns) {
			if (nc.getIn(sc, tab) == null)
				return null;
		}
		return "Table " + tab.getName() + " already contains column" + (newColumns.size() > 1 ? "s " : " ") +
				Functional.join(newColumns, ", ", new UnaryFunction<String,PEColumn>() {

					@Override
					public String evaluate(PEColumn object) {
						return object.getName().get();
					}
					
				})
				+ ", cannot add";
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		return new AddColumnAction(newColumns);
	}

	@Override
	public Action getActionKind() {
		return Action.CREATE;
	}

	@Override
	public List<PEColumn> getColumns() {
		return newColumns;
	}

	public Pair<String, Name> getFirstOrAfterSpec() {
		return firstOrAfterSpec;
	}

}

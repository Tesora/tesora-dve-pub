package com.tesora.dve.tools.analyzer.stats;

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

import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.util.ListSet;

public class DeleteStatementAnalysis extends StatementAnalysis<DeleteStatement> {

	public DeleteStatementAnalysis(SchemaContext sc, String sql, int freq, DeleteStatement stmt) {
		super(sc, sql, freq, stmt);
	}

	@Override
	public void visit(StatsVisitor sv) {
		super.visit(sv);
		final ListSet<PETable> tabs = new ListSet<PETable>();
		for (final TableInstance ti : getStatement().getTargetDeleteEdge()) {
			final Table<?> table = ti.getTable();
			addPETable(table, tabs);
		}
		if (tabs.isEmpty()) {
			final Table<?> table = getStatement().getTablesEdge().get(0).getBaseTable().getTable();
			addPETable(table, tabs);
		}
		sv.onDelete(tabs, frequency);
	}

	private void addPETable(final Table<?> table, final ListSet<PETable> tables) {
		if (table instanceof PETable) {
			tables.add((PETable) table);
		}
	}
}

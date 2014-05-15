// OS_STATUS: public
package com.tesora.dve.tools.analyzer.stats;

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

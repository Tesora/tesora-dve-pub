// OS_STATUS: public
package com.tesora.dve.tools.analyzer.stats;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;

public class InsertStatementAnalysis extends StatementAnalysis<InsertStatement> {

	public InsertStatementAnalysis(SchemaContext db, String sql, int freq, InsertStatement stmt) {
		super(db, sql, freq, stmt);
	}

	@Override
	public List<Table<?>> getTables() {
		// if an insert into select, the tables on the nested select are counted elsewhere
		final PETable pet = getStatement().getTable().asTable();
		final ArrayList<Table<?>> ugh = new ArrayList<Table<?>>();
		ugh.add(pet);
		return ugh;
	}

	@Override
	public void visit(StatsVisitor sv) {
		super.visit(sv);
		final InsertStatement is = getStatement();
		if (is instanceof InsertIntoValuesStatement) {
			final InsertIntoValuesStatement iivs = (InsertIntoValuesStatement) is;
			final Table<?> table = is.getTableInstance().getTable();
			if (table instanceof PETable) {
				sv.onInsertValues((PETable) table, iivs.getValues().size(), frequency);
			}
		}
	}
}

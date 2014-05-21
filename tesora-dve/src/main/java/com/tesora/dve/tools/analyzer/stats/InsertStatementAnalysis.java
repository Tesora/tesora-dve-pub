// OS_STATUS: public
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

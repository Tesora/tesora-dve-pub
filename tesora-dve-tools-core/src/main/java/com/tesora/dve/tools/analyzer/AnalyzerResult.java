package com.tesora.dve.tools.analyzer;

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

import java.io.PrintStream;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public abstract class AnalyzerResult {

	private final SchemaContext context;
	private final String originalSQL;
	private final SourcePosition position;
	private final DMLStatement stmt;

	protected AnalyzerResult(final SchemaContext sc, final String sql, final SourcePosition pos, final DMLStatement s) {
		context = sc;
		originalSQL = sql;
		stmt = s;
		position = pos;
	}

	public long getStatementLineNumber() {
		return position.getPosition();
	}

	public String getOriginalSQL() {
		return originalSQL;
	}

	public void printTables(final PrintStream ps) {
		if (stmt != null) {
			final Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
			emitter.setOptions(EmitOptions.PEMETADATA);

			ps.println("Tables:");

			for (final TableKey tk : stmt.getDerivedInfo().getAllTableKeys()) {
				final StringBuilder buf = new StringBuilder();
				if (tk instanceof MTTableKey) {
					buf.append("  '" + ((MTTableKey) tk).getScope().getName().get() + "'");
				} else {
					buf.append("  '" + tk.getTable().getName().get() + "'");
				}
				if (tk.getTable() instanceof PETable) {
					emitter.emitDeclaration(context, tk.getAbstractTable().getDistributionVector(context), buf);
				}
				ps.println(buf.toString());
			}
		}
	}

	public void printStatement(final PrintStream ps) {
		position.describe(ps);
		ps.println(originalSQL);
	}

	protected SchemaContext getSchemaContext() {
		return this.context;
	}
}

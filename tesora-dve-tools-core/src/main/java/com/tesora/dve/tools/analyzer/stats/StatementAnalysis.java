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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

public class StatementAnalysis<T extends DMLStatement> {

	protected String sql;
	protected int frequency;

	protected T stmt;

	protected final SchemaContext sc;

	public StatementAnalysis(SchemaContext sc, String sql, int freq, T stmt) {
		this.sql = sql;
		this.frequency = freq;
		this.stmt = stmt;
		this.sc = sc;
	}

	public T getStatement() {
		return stmt;
	}

	public SchemaContext getSchemaContext() {
		return sc;
	}

	public List<Table<?>> getTables() {
		final ListSet<TableKey> tabs = EngineConstant.TABLES.getValue(stmt, sc);
		return Functional.apply(tabs, new UnaryFunction<Table<?>, TableKey>() {

			@Override
			public Table<?> evaluate(TableKey object) {
				return object.getTable();
			}

		});
	}

	public List<PETable> getPETables() {
		final List<PETable> tabs = new ArrayList<PETable>();
		for (final Table<?> table : getTables()) {
			if (table instanceof PETable) {
				tabs.add((PETable) table);
			}
		}

		return tabs;
	}

	public List<EquijoinInfo> getJoinInfo() {
		return Collections.emptyList();
	}

	/**
	 * A column from the where clause of the form c = <literal>, or c = ?.
	 * 
	 * @see StatsVisitor
	 */
	public Set<Column<?>> getIdentityColumns() {
		final IdentityClauseTraversal ict = new IdentityClauseTraversal();
		ict.traverse(EngineConstant.WHERECLAUSE.getEdge(getStatement()));
		return ict.getIdentityColumns();
	}

	public void visit(StatsVisitor sv) {
		if (stmt instanceof SelectStatement) {
			sv.onSelect(getTables(), frequency);
		} else if (stmt instanceof UpdateStatement) {
			sv.onUpdate(getPETables(), frequency);
		} else if (stmt instanceof InsertIntoSelectStatement) {
			final InsertStatement is = (InsertStatement) stmt;
			final Table<?> table = is.getTableInstance().getTable();
			if (table instanceof PETable) {
				sv.onInsertIntoSelect((PETable) table, frequency);
			}
		} else if (stmt instanceof UnionStatement) {
			sv.onUnion(frequency);
		}

		final List<EquijoinInfo> joins = getJoinInfo();
		for (final EquijoinInfo eji : joins) {
			sv.onJoin(eji, frequency);
		}

		final Set<Column<?>> identCols = getIdentityColumns();
		for (final Column<?> p : identCols) {
			sv.onIdentColumn(p, frequency);
		}
	}

	private final String nl = System.getProperty("line.separator");

	public void emit(String indent, List<String> lines) {
		lines.add(indent + stmt.getClass().getSimpleName() + "[" + frequency + "]");
		if (sql != null) {
			lines.add(indent + sql);
		} else {
			lines.add(indent + "(nested)");
		}
		final List<Table<?>> tabs = getTables();
		if (tabs.size() == 1) {
			lines.add(indent + "Table: " + tabs.get(0).getName().getSQL());
		} else {
			lines.add(indent + "Tables: ");
			for (final Table<?> p : tabs) {
				lines.add(indent + "  " + p.getName().getSQL());
			}
		}
		final List<EquijoinInfo> joins = getJoinInfo();
		if (!joins.isEmpty()) {
			lines.add(indent + "Equijoins");
			for (final EquijoinInfo eji : joins) {
				lines.add(indent + "   " + eji.getLHS().getName() + " join " + eji.getRHS().getName());
				for (final Pair<PEColumn, PEColumn> p : eji.getEquijoins()) {
					lines.add(indent + "      " + p.getFirst().getName() + " = " + p.getSecond().getName());
				}
			}
		}
		final Set<Column<?>> identCols = getIdentityColumns();
		if (!identCols.isEmpty()) {
			lines.add(indent + "Identity columns");
			for (final Column<?> pec : identCols) {
				final Name qname = pec.getName().postfix(pec.getTable().getName());
				final StringBuilder buf = new StringBuilder();
				if (pec instanceof PEColumn) {
					Singletons.require(HostService.class).getDBNative().getEmitter().emitDeclaration(((PEColumn) pec).getType(), (PEColumn) pec, buf, false);
				}
				lines.add(indent + "   " + qname.getSQL() + " " + buf.toString());
			}
		}
	}

	@Override
	public String toString() {
		final ArrayList<String> lines = new ArrayList<String>();
		emit("", lines);
		final StringBuffer buf = new StringBuffer();
		for (final String s : lines) {
			buf.append(s).append(nl);
		}
		return buf.toString();
	}

	public static class IdentityClauseTraversal extends Traversal {

		private final Set<Column<?>> identColumns = new LinkedHashSet<Column<?>>();

		public IdentityClauseTraversal() {
			super(Order.POSTORDER, ExecStyle.ONCE);
		}

		public Set<Column<?>> getIdentityColumns() {
			return identColumns;
		}

		@Override
		public int hashCode() {
			return identColumns.hashCode();
		}

		@Override
		public boolean equals(final Object other) {
			if (other == this) {
				return true;
			} else if (other instanceof IdentityClauseTraversal) {
				final IdentityClauseTraversal otherTraversal = (IdentityClauseTraversal) other;
				return otherTraversal.identColumns.equals(this.identColumns);
			}

			return false;
		}

		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.FUNCTION.has(in)) {
				final FunctionCall fc = (FunctionCall) in;
				if (fc.getFunctionName().isEquals()) {
					final ExpressionNode lhs = fc.getParametersEdge().get(0);
					final ExpressionNode rhs = fc.getParametersEdge().get(1);
					if ((lhs instanceof ColumnInstance) && (rhs instanceof ConstantExpression)) {
						final ColumnInstance ci = (ColumnInstance) lhs;
						identColumns.add(ci.getColumn());
					} else if ((lhs instanceof ConstantExpression) && (rhs instanceof ColumnInstance)) {
						final ColumnInstance ci = (ColumnInstance) rhs;
						identColumns.add(ci.getColumn());
					}
				} else if (fc.getFunctionName().isIn()) {
					final ExpressionNode lhs = fc.getParametersEdge().get(0);
					if (lhs instanceof ColumnInstance) {
						final ColumnInstance ci = (ColumnInstance) lhs;
						identColumns.add(ci.getColumn());
					}
				}
			}
			return in;
		}

	}

}

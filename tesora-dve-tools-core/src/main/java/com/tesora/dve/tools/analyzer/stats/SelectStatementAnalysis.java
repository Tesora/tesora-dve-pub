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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.jg.JoinEdge;
import com.tesora.dve.sql.jg.UncollapsedJoinGraph;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.tools.analyzer.StatementCounter;

public class SelectStatementAnalysis extends StatementAnalysis<SelectStatement> {

	private static final Logger logger = Logger.getLogger(StatementCounter.class);

	public SelectStatementAnalysis(SchemaContext sc, String sql, int freq, SelectStatement stmt) {
		super(sc, sql, freq, stmt);
	}

	@Override
	public List<EquijoinInfo> getJoinInfo() {
		final UncollapsedJoinGraph ujg = new UncollapsedJoinGraph(getSchemaContext(), getStatement(), true);
		final ArrayList<EquijoinInfo> out = new ArrayList<EquijoinInfo>();
		for (final JoinEdge je : ujg.getJoins()) {
			final JoinedTable jt = je.getJoin().getJoin();
			final List<Name> usingSpec = (jt != null) ? jt.getUsingColSpec() : null;
			final ExpressionNode onClause = (jt != null) ? jt.getJoinOn() : null;
			final EquijoinInfo eji = new EquijoinInfo(je.getLHSTab().getAbstractTable(), je.getRHSTab().getAbstractTable(), je.getJoinType(),
					usingSpec, onClause, sc);
			for (final Pair<ColumnInstance, ColumnInstance> p : je.getSimpleColumns()) {
				eji.addJoinExpression(p.getFirst(), p.getSecond());
			}
			out.add(eji);
		}
		return out;
	}

	@Override
	public void visit(StatsVisitor sv) {
		super.visit(sv);
		// have to normalize first to get the column names and aliases right
		final SelectStatement stmt = getStatement();
		try {
			stmt.normalize(getSchemaContext());
		} catch (final SchemaException se) {
			logger.warn("Could not normalize SELECT statement: " + stmt.getSQL(getSchemaContext()));
			return;
		}

		/* On Group By edge. */
		final Set<Column<?>> groupByTuple = new LinkedHashSet<Column<?>>();
		for (final SortingSpecification ss : getStatement().getGroupBysEdge()) {
			if (ss.getTarget() instanceof AliasInstance) {
				final AliasInstance ai = (AliasInstance) ss.getTarget();
				final ExpressionAlias ea = ai.getTarget();
				final ExpressionNode actual = ExpressionUtils.getTarget(ea);
				if (actual instanceof ColumnInstance) {
					final ColumnInstance ci = (ColumnInstance) actual;
					final Column<?> c = ci.getColumn();
					groupByTuple.add(c);
					sv.onGroupBy(c, frequency);
				}
			} else if (ss.getTarget() instanceof ColumnInstance) {
				final ColumnInstance ci = (ColumnInstance) ss.getTarget();
				final Column<?> c = ci.getColumn();
				groupByTuple.add(c);
				sv.onGroupBy(c, frequency);
			}
		}
		
		if (!groupByTuple.isEmpty()) {
			sv.onGroupByColumnTuple(groupByTuple, frequency);
		}

		/*
		 * On Order By edge.
		 * Count only select statements involving a single table.
		 * Statements with joins would benefit only if all the involved
		 * tables were broadcast.
		 * Multiple sorting columns count as one.
		 */
		if (getStatement().getOrderBysEdge().has()) {
			final Set<Table<?>> uniqueSortedTables = new HashSet<Table<?>>(getTables());
			if (uniqueSortedTables.size() == 1) {
				final Table<?> table = uniqueSortedTables.iterator().next();
				if (table instanceof PETable) {
					sv.onOrderBy((PETable) table, frequency);
				}
			}
		}
	}

}

package com.tesora.dve.sql.transform.strategy;

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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.MultiTableDMLStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

public class NaturalJoinRewriteTransformFactory extends TransformFactory {

	private static boolean applies(final DMLStatement stmt) {
		return EngineConstant.FROMCLAUSE.has(stmt) && (stmt instanceof MultiTableDMLStatement);
	}

	@Override
	public FeatureStep plan(final DMLStatement stmt, final PlannerContext context) throws PEException {
		if (!applies(stmt)) {
			return null;
		}
		
		final MultiTableDMLStatement transformed = (MultiTableDMLStatement) CopyVisitor.copy(stmt);
		final Set<JoinedTable> joins = collectNaturalJoins(transformed);
		if (joins.isEmpty()) {
			return null;
		}

		final SchemaContext sc = context.getContext();
		for (final JoinedTable join : joins) {
			rewriteToInnerJoin(sc, join);
		}

		return buildPlan(transformed, context, DefaultFeaturePlannerFilter.INSTANCE);
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.NATURAL_JOIN_REWRITE;
	}

	private Set<JoinedTable> collectNaturalJoins(final MultiTableDMLStatement dmls) {
		final List<FromTableReference> joinedTables = dmls.getTables();
		final Set<JoinedTable> naturalJoins = new LinkedHashSet<JoinedTable>();
		for (final FromTableReference ftr : joinedTables) {
			for (final JoinedTable tableJoin : ftr.getTableJoins()) {
				if (tableJoin.getJoinType().isNaturalJoin()) {
					naturalJoins.add(tableJoin);
				}
			}
		}

		return naturalJoins;
	}

	private void rewriteToInnerJoin(final SchemaContext sc, final JoinedTable join) throws PEException {
		final TableInstance base = join.getEnclosingFromTableReference().getBaseTable();
		final TableInstance target = join.getJoinedToTable();

		final Edge<?, ExpressionNode> joinOnEdge = join.getJoinOnEdge();
		final Map<ExpressionNode, ExpressionNode> naturalPairs = collectNaturalJoinColumnProjections(sc, base, target);
		for (final Map.Entry<ExpressionNode, ExpressionNode> equiPair : naturalPairs.entrySet()) {
			final ExpressionNode lhs = equiPair.getKey();
			final ExpressionNode rhs = equiPair.getValue();
			joinOnEdge.add(new FunctionCall(FunctionName.makeEquals(), lhs, rhs));
		}

		final JoinSpecification originalJoinKind = join.getJoinType();
		if (originalJoinKind.isInnerJoin()) {
			join.setJoinType(JoinSpecification.INNER_JOIN);
		} else if (originalJoinKind.isLeftOuterJoin()) {
			join.setJoinType(JoinSpecification.LEFT_OUTER_JOIN);
		} else {
			throw new PEException("No natural join rewrite available for '" + originalJoinKind + "' join kind.");
		}
	}

	private Map<ExpressionNode, ExpressionNode> collectNaturalJoinColumnProjections(final SchemaContext sc, final TableInstance base,
			final TableInstance target) {
		final Map<String, ExpressionNode> lhsTabCols = getColumnsProjectionsByName(sc, base);
		final Map<String, ExpressionNode> rhsTabCols = getColumnsProjectionsByName(sc, target);

		final Map<ExpressionNode, ExpressionNode> naturalColumns = new LinkedHashMap<ExpressionNode, ExpressionNode>();
		for (final String lhsColName : lhsTabCols.keySet()) {
			final ExpressionNode rhsColumn = rhsTabCols.get(lhsColName);
			if (rhsColumn != null) {
				final ExpressionNode lhsColumn = lhsTabCols.get(lhsColName);
				naturalColumns.put(lhsColumn, rhsColumn);
			}
		}

		return naturalColumns;
	}

	private Map<String, ExpressionNode> getColumnsProjectionsByName(final SchemaContext sc, final TableInstance table) {
		final Map<String, ExpressionNode> columnsByName = new LinkedHashMap<String, ExpressionNode>();
		for (final PEColumn column : table.getAbstractTable().getColumns(sc)) {
			final String columnName = column.getName().getUnqualified().getUnquotedName().get();
			columnsByName.put(columnName, column.buildProjection(table));
		}

		return columnsByName;
	}

}

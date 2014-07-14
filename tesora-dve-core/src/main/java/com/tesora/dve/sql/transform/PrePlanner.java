package com.tesora.dve.sql.transform;

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



import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.variables.KnownVariables;

public class PrePlanner {

	public static Statement transform(SchemaContext sc, Statement in) throws SchemaException {
		if (in == null) return in;
		Statement c = in;
		if (EngineConstant.WHERECLAUSE.has(c)) {
			// in the where clause, convert 1 = a to a = 1
			// also restructure and/or to be broader
			new FilterOrder().traverse(EngineConstant.WHERECLAUSE.getEdge(c));
		}
		c = maybeRewriteNested(sc, c);
		c = maybeAddLimitClause(sc, c);
		return c;
	}
		
	private static Statement maybeAddLimitClause(SchemaContext sc, Statement c) {
		if (c instanceof SelectStatement) {
			SelectStatement ss = (SelectStatement) c;
			if (ss.getLimitEdge().has())
				return c;
			Long limitByOtherMeans = 
						KnownVariables.SELECT_LIMIT.getValue(sc.getConnection().getVariableSource());
			if (limitByOtherMeans == null)
				return c;
			ss.setLimit(new LimitSpecification(LiteralExpression.makeLongLiteral(limitByOtherMeans.longValue()),
					LiteralExpression.makeAutoIncrLiteral(0L)));
		}
		return c;
	}
	
	// rewrite select count(*) as expression from (select 1 as expression from .... ) subquery
	// to be select count(*) as expression from .....
	private static Statement maybeRewriteNested(SchemaContext sc, Statement c) {
		if (!(c instanceof SelectStatement)) return c;
		ListSet<ProjectingStatement> chilluns = EngineConstant.NESTED.getValue(c,sc);
		if (chilluns == null || chilluns.isEmpty() || chilluns.size() > 1) return c;
		SelectStatement outer = (SelectStatement) c;
		if (outer.getWhereClause() != null || outer.getLimit() != null || outer.getOrderBysEdge().has() || outer.getGroupBysEdge().has())
			return c;
		ProjectingStatement ps = chilluns.get(0);
		if (ps instanceof UnionStatement) return c;
		SelectStatement inner = (SelectStatement) ps;
		if (outer.getProjectionEdge().size() == 1 && inner.getProjectionEdge().size() == 1) {
			ExpressionNode innerProj = inner.getProjectionEdge().get(0);
			ExpressionNode outerProj = outer.getProjectionEdge().get(0);
			ExpressionNode ip = innerProj;
			ExpressionNode op = outerProj;
			if (op instanceof ExpressionAlias)
				op = ((ExpressionAlias)op).getTarget();
			if (ip instanceof ExpressionAlias)
				ip = ((ExpressionAlias)ip).getTarget();
			if (ip instanceof LiteralExpression && EngineConstant.FUNCTION.has(op, EngineConstant.COUNT)) {
				FunctionCall fcp = (FunctionCall) op;
				if (fcp.getParametersEdge().size() == 1 && fcp.getParametersEdge().get(0) instanceof Wildcard) {
					// we're just going to replace the literal with the funcall by replacing ip with op and returning the
					// inner select, but we have to preserve the aliases, so let's swap the eas
					innerProj.getParentEdge().set(outerProj);
					// disconnect inner from the parent - otherwise we'll match nested again
					inner.setParent(null);
					inner.setExplain(outer.getExplain());
					// additionally, we're going to replace count(*) with count(pk), or at least the first
					// column of the pk
					TableInstance firstTable = inner.getTablesEdge().get(0).getBaseTable();
					if (firstTable != null && firstTable.getAbstractTable().isTable()) {
						PETable pet = firstTable.getAbstractTable().asTable();
						PEKey k = pet.getPrimaryKey(sc);
						if (k != null) {
							ColumnInstance nci = new ColumnInstance(k.getColumns(sc).get(0),firstTable);
							fcp.getParametersEdge().set(nci);
						}
					}
					return inner;
				}
			}
		}
		return c;
	}

	private static class FilterOrder extends Traversal {
		
		public FilterOrder() {
			super(Order.POSTORDER, ExecStyle.ONCE);
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.FUNCTION.has(in, EngineConstant.EQUALS)) {
				FunctionCall fc = (FunctionCall) in;
				if (EngineConstant.CONSTANT.has(fc.getParametersEdge().get(0)) &&
						EngineConstant.COLUMN.has(fc.getParametersEdge().get(1))) {
					ExpressionNode lhs = fc.getParametersEdge().get(0);
					ExpressionNode rhs = fc.getParametersEdge().get(1);
					fc.getParametersEdge().getEdge(1).set(lhs);
					fc.getParametersEdge().getEdge(0).set(rhs);
				}
			}
			return in;
		}
		
	}
	
}

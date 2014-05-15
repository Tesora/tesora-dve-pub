// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.nested;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.MultiTableDMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.util.ListSet;

/*
 * We essentially only handle two cases here:
 * [1] where p1 in (subq) => inner join
 * [2] where p1 not in (subq) => left join with is null 
 */
public class HandleWhereClauseSubqueryAsJoin extends StrategyFactory {

	@Override
	public NestingStrategy adapt(SchemaContext sc, EdgeTest location,
			DMLStatement enclosing, Subquery sq, ExpressionPath path)
			throws PEException {
		if (sq.getStatement() instanceof UnionStatement)
			return null;
		if (location != EngineConstant.WHERECLAUSE)
			return null;
		LanguageNode ln = sq.getParent();
		if (EngineConstant.FUNCTION.has(ln, EngineConstant.IN) || EngineConstant.FUNCTION.has(ln,EngineConstant.NOTIN)) {
			return new WhereClauseSubqueryHandler(sq,path);
		}
		return null;
	}

	protected static class WhereClauseSubqueryHandler extends NestingStrategy {

		public WhereClauseSubqueryHandler(Subquery nested, ExpressionPath pathTo) {
			super(nested, pathTo);
		}

		@Override
		public DMLStatement beforeChildPlanning(SchemaContext sc, DMLStatement orig)
				throws PEException {
			MultiTableDMLStatement out = (MultiTableDMLStatement) orig;

			boolean handled = false;
			LanguageNode ln = sq.getParent(); 
			if (EngineConstant.FUNCTION.has(ln, EngineConstant.IN)) {
				FunctionCall incall = (FunctionCall)ln;
				if (EngineConstant.FUNCTION.has(incall.getParent(), EngineConstant.NOT)) {
					// not in
					out = subNotIn(out);
					handled = true;
				} else {
					// in
					out = subIn(out);
					handled = true;
				}
			} else if (EngineConstant.FUNCTION.has(ln, EngineConstant.NOTIN)) {
				out = subNotIn(out);
				handled = true;
			}
			if (!handled)
				throw new PEException("Unhandled nested query: " 
						+ sq.getStatement().getSQL(sc) + " in " + orig.getSQL(sc));
			return out;
		}

		private MultiTableDMLStatement subIn(MultiTableDMLStatement dmls) {
			SelectStatement sub = (SelectStatement) sq.getStatement(); 
			uniqueAliases(sub,dmls);
			LanguageNode ln = sub.getParent().getParent();
			FunctionCall fc = (FunctionCall)ln;
			ColumnInstance ci = (ColumnInstance) fc.getParametersEdge().get(0);
			// we know, by virtue of it being an in expression, that the subquery must return a single column
			// so, we're going to add the from clause of the subquery to the from clauses of the parent query,
			// add the where clause of the sub query to the where clause of the parent query
			// and add ci = (proj expr) to the where clause of the parent query
			
			ExpressionNode projEntry = ExpressionUtils.getTarget(sub.getProjectionEdge().get(0));
			FunctionCall repl = new FunctionCall(FunctionName.makeEquals(),ci,projEntry);
			fc.getParentEdge().set(repl);
			
			MultiEdge<?, LanguageNode> fromEdge = (MultiEdge<?, LanguageNode>) EngineConstant.FROMCLAUSE.getEdge(dmls);
			for(FromTableReference ftr : sub.getTablesEdge().getMulti())
				fromEdge.add((LanguageNode)ftr);
			Edge<?, LanguageNode> wcEdge = EngineConstant.WHERECLAUSE.getEdge(dmls);
			ExpressionNode dmlswc = (ExpressionNode) wcEdge.get();
			List<ExpressionNode> decompwc = ExpressionUtils.decomposeAndClause(dmlswc);
			if (decompwc.isEmpty())
				decompwc.add(dmlswc);
			ExpressionNode subswc = sub.getWhereClause();
			if (subswc != null) {
				decompwc.add(subswc);
			}
			if (decompwc.size() == 1)
				wcEdge.set(decompwc.get(0));
			else
				wcEdge.set(ExpressionUtils.buildAnd(decompwc));
			dmls.getDerivedInfo().addLocalTables(sq.getStatement().getDerivedInfo().getLocalTableKeys());
			dmls.getDerivedInfo().getLocalNestedQueries().remove(sq.getStatement());
			return dmls;
		}

		private MultiTableDMLStatement subNotIn(MultiTableDMLStatement dmls) throws PEException {
			SelectStatement sub = (SelectStatement) sq.getStatement();
			uniqueAliases(sub,dmls);
			LanguageNode ln = sub.getParent().getParent();
			FunctionCall fc = (FunctionCall)ln;
			LanguageNode parent = fc.getParent();
			Edge<?, ExpressionNode> toSet = null;
			if (fc.getFunctionName().isIn()) {
				// not (a in ...)
				toSet = parent.getParentEdge();
			} else if (fc.getFunctionName().isNotIn()) {
				toSet = fc.getParentEdge();
			} else {
				throw new PEException("Unsupported in expression for nested query rewrite");
			}
			ColumnInstance ci = (ColumnInstance) fc.getParametersEdge().get(0);
			// build a left outer join between the lhs and the rhs
			// and replace the in clause with an is null on the rhs
			ExpressionNode projEntry = ExpressionUtils.getTarget(sub.getProjectionEdge().get(0));
			FunctionCall repl = new FunctionCall(FunctionName.makeIs(), (ExpressionNode)projEntry.copy(null), LiteralExpression.makeNullLiteral());
			toSet.set(repl);
			TableInstance rti = null;
			if (projEntry instanceof ColumnInstance) {
				ColumnInstance pci = (ColumnInstance) projEntry;
				rti = pci.getTableInstance();
			} else {
				throw new PEException("Unsupported complex scalar expression for not in");
			}
			
			TableInstance lti = ci.getTableInstance();
			TableKey ltk = lti.getTableKey();
			// search the ftrs for lti, then add the outer join to that ftr
			// add all the nested query ftrs to the parent
			ArrayList<ExpressionNode> joinExs = new ArrayList<ExpressionNode>();
			joinExs.add(new FunctionCall(FunctionName.makeEquals(), (ExpressionNode)ci.copy(null), (ExpressionNode)projEntry.copy(null)));
			if (sub.getWhereClause() != null)
				joinExs.add(sub.getWhereClause());
			JoinedTable njt = new JoinedTable(rti,ExpressionUtils.buildWhereClause(joinExs),JoinSpecification.LEFT_OUTER_JOIN);
			
			FromTableReference newHome = null;
			
			for(FromTableReference ftr : dmls.getTablesEdge().getMulti()) {
				if (ftr.getBaseTable() != null && ftr.getBaseTable().getTableKey().equals(ltk)) {
					ftr.addJoinedTable(njt);
					newHome = ftr;
					break;
				} else {
					boolean matches = false;
					for(JoinedTable jt : ftr.getTableJoins()) {
						if (jt.getJoinedTo() instanceof TableInstance) {
							TableInstance ti = (TableInstance) jt.getJoinedTo();
							if (ti.getTableKey().equals(ltk)) {
								matches = true;
								break;
							}
						}
					}
					if (matches) {
						ftr.addJoinedTable(njt);
						newHome = ftr;
						break;
					}
				}
			}
			
			TableKey rtk = rti.getTableKey();
			
			MultiEdge<?, LanguageNode> fromEdge = (MultiEdge<?, LanguageNode>) EngineConstant.FROMCLAUSE.getEdge(dmls);
			for(FromTableReference ftr : sub.getTablesEdge().getMulti()) {
				boolean add = true;
				if (ftr.getBaseTable() != null && ftr.getBaseTable().getTableKey().equals(rtk)) {
					if (!ftr.getTableJoins().isEmpty()) {
						// we're just going to add all these to the new home
						newHome.addJoinedTable(ftr.getTableJoins());
					}
					add = false;
				} else {
					int containing = -1;
					List<JoinedTable> tableJoins = ftr.getTableJoins();
					for(int i = 0; i < tableJoins.size(); i++) {
						JoinedTable jt = tableJoins.get(i);
						if (jt.getJoinedTo() instanceof TableInstance) {
							TableInstance ti = (TableInstance) jt.getJoinedTo();
							if (ti.getTableKey().equals(rtk)) {
								containing = i;
								break;
							}
						}
					}
					if (containing > -1)
						ftr.removeJoinedTable(containing);
				}
				if (add)
					fromEdge.add((LanguageNode)ftr);
			}
			dmls.getDerivedInfo().addLocalTables(sq.getStatement().getDerivedInfo().getLocalTableKeys());
			dmls.getDerivedInfo().getLocalNestedQueries().remove(sq.getStatement());
			return dmls;
		}

		private void uniqueAliases(SelectStatement subq, MultiTableDMLStatement dmls) {
			ListSet<TableKey> nestedTabs = subq.getDerivedInfo().getLocalTableKeys();
			AliasInformation pin = dmls.getAliases();
			LinkedHashMap<TableKey,UnqualifiedName> replacement = new LinkedHashMap<TableKey,UnqualifiedName>();
			for(TableKey tk : nestedTabs) {
				TableInstance ti = tk.toInstance();
				if (ti.getAlias() == null) continue;
				if (pin.getAliasCount(ti.getAlias()) > 0) {
					replacement.put(tk,pin.buildNewAlias(ti.getAlias()));
				}
			}
			if (replacement.isEmpty()) return;
			new AliasReplacement(replacement).traverse(subq);
		}
		
		private static class AliasReplacement extends Traversal {

			private final Map<TableKey,UnqualifiedName> forwarding;
			
			public AliasReplacement(Map<TableKey,UnqualifiedName> forwarding) {
				super(Order.POSTORDER, ExecStyle.ONCE);
				this.forwarding = forwarding;
			}

			@Override
			public LanguageNode action(LanguageNode in) {
				if (in instanceof TableInstance) {
					TableInstance ti = (TableInstance) in;
					updateTable(ti);
				} else if (in instanceof ColumnInstance) {
					ColumnInstance ci = (ColumnInstance) in;
					TableInstance ti = ci.getTableInstance();
					updateTable(ti);
				}
				return in;
			}
			
			private void updateTable(TableInstance ti) {
				UnqualifiedName unq = forwarding.get(ti.getTableKey());
				if (unq != null)
					ti.setAlias(unq, false);
			}
			
		}
		
	}
	
	

}

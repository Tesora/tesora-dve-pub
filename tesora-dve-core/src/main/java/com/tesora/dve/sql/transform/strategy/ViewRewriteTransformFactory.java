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


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.ViewMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.AbstractTraversal.ExecStyle;
import com.tesora.dve.sql.node.AbstractTraversal.Order;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TableJoin;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.TableInstanceCollector;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.util.ListSet;

/*
 * The view rewrite swaps in the view definition, possibly merging it into the parent query 
 * if possible.  It may not be possible if the view definition involves agg funs, a distinct
 * set quantifier, a group by, having, limit, or order by clause, etc.  In that case we 
 * simply swap the view query in as a subquery.
 */
public class ViewRewriteTransformFactory extends TransformFactory {

	
	private boolean applies(SchemaContext sc, 
			DMLStatement stmt) throws PEException {
		for(TableKey tk : stmt.getDerivedInfo().getAllTableKeys()) {
			Table<?> t = tk.getTable();
			if (t.isInfoSchema()) continue;
			PEAbstractTable<?> peat = (PEAbstractTable<?>) t;
			if (peat.isView() && peat.asView().getView(sc).getMode() == ViewMode.EMULATE)
				return true;
		}
		return false;
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.VIEW;
	}
	
	public static void applyViewRewrites(SchemaContext sc, DMLStatement dmls, FeaturePlanner planner) {
		boolean any;
		do {
			any = false;
			List<TableInstance> tables = TableInstanceCollector.getInstances(dmls);
			for(TableInstance ti : tables) {
				if (ti.getTable().isInfoSchema()) continue;
				PEAbstractTable<?> peat = (PEAbstractTable<?>) ti.getTable();
				if (peat.isTable()) continue;
				PEViewTable petv = peat.asView();
				if (petv.getView(sc).getMode() == ViewMode.ACTUAL) continue;
				swapInView(sc,dmls,ti,petv,dmls.getAliases());
				if (planner != null && planner.emitting()) {
					planner.emit("After swapping in " + petv.getName() + " for " + ti);
					planner.emit(dmls.getSQL(sc, "  "));
				}
				any = true;
			}
		} while(any);		
	}
	
	private static void swapInView(SchemaContext sc, DMLStatement dmls, TableInstance ti, PEViewTable petv, AliasInformation ai) {
		ProjectingStatement canonical = petv.getView(sc).getViewDefinition(sc,petv,true);
		ProjectingStatement copy = CopyVisitor.copy(canonical);
		ProjectingStatement remapped = remap(sc,copy,dmls);
		if (ti.getAlias() == null) 
			ti.setAlias(ai.buildNewAlias(new UnqualifiedName("vsq")));
		// I have a remapped stmt, see if I can do a merge
		Boolean merge = petv.getView(sc).isMerge(sc,petv);
		if (merge == null)
			// assume we can't then
			merge = false;
		if (merge.booleanValue()) {
			remapped = merge(sc, dmls,ti,petv,remapped);
		} else {
			ensureMapped(sc, dmls, ti, petv, remapped); 
		}
		if (remapped == null) {
			// we completely merged the definition into dmls - we're done
			return;
		}
		Subquery sq = new Subquery(remapped,ti.getAlias(),ti.getSourceLocation());
		sq.setTable(petv);
		// find the nearest enclosing statement - we need to update the derived info
		ProjectingStatement ancestor = ti.getEnclosing(ProjectingStatement.class, ProjectingStatement.class);
		if (ancestor == null)
			throw new SchemaException(Pass.PLANNER, "Unable to find enclosing stmt for table instance");
		ancestor.getDerivedInfo().addNestedStatements(Collections.singleton(remapped));
		ancestor.getDerivedInfo().removeLocalTable(petv);
		ancestor.getDerivedInfo().clearCorrelatedColumns();
		// finally, swap out the table instance for the subquery
		ti.getParentEdge().set(sq);
	}

	private static ProjectingStatement remap(SchemaContext sc, ProjectingStatement ps, DMLStatement enclosing) {
		NodeUpdater updater = new NodeUpdater(sc,enclosing);
		updater.traverse(ps);
		ps.getDerivedInfo().remap(updater.getForwarding());
		return ps;
	}
	
	// the first thing we need is a traversal to forward all the table nodes
	private static class NodeUpdater extends Traversal {

		private final HashMap<Long,Long> forwarding;
		private final SchemaContext cntxt;
		private final AliasInformation existing;
		private final HashMap<UnqualifiedName,UnqualifiedName> tableAliasForwarding;
		
		public NodeUpdater(SchemaContext sc, DMLStatement dmls) {
			super(Order.PREORDER,ExecStyle.ONCE);
			forwarding = new HashMap<Long,Long>();
			cntxt = sc;
			existing = dmls.getAliases();
			tableAliasForwarding = new HashMap<UnqualifiedName,UnqualifiedName>();
		}
		
		public Map<Long,Long> getForwarding() {
			return forwarding;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof TableInstance) {
				TableInstance ti = (TableInstance) in;
				long was = ti.getNode();
				Long now = forwarding.get(was);
				if (now == null) {
					now = cntxt.getNextTable();
					forwarding.put(was, now);
				}
				ti.setNode(now);
				UnqualifiedName alias = ti.getAlias();
				if (alias == null)
					return in;
				int extant = existing.getAliasCount(alias);
				if (extant > 0) {
					UnqualifiedName repl = tableAliasForwarding.get(alias);
					if (repl == null) {
						repl = existing.buildNewAlias(alias);
						tableAliasForwarding.put(alias, repl);
					}
					ti.setAlias(repl,false);
				}
			}
			return in;
		}
		
	}

	// this is the non merge case, so we want to ensure that all refs to the view table are replaced with ti - i.e. all column refs
	private static void ensureMapped(final SchemaContext sc, DMLStatement enclosing, final TableInstance ti, final PEViewTable theView, ProjectingStatement remapped) {
		final TableKey myKey = ti.getTableKey();
		new Traversal(Order.POSTORDER, ExecStyle.ONCE) {

			@Override
			public LanguageNode action(LanguageNode in) {
				if (in instanceof ColumnInstance) {
					ColumnInstance ci = (ColumnInstance) in;
					if (ci.getTableInstance().getTableKey().equals(myKey)) {
						return new ColumnInstance(ci.getSpecifiedAs(),ci.getColumn(),ti);
					}
				}
				return in;
			}
			
		}.traverse(enclosing);
		enclosing.getDerivedInfo().removeLocalTable(theView);
		enclosing.getDerivedInfo().addLocalTables(remapped.getDerivedInfo().getLocalTableKeys());
		enclosing.getDerivedInfo().addNestedStatements(remapped.getDerivedInfo().getLocalNestedQueries());
		enclosing.getDerivedInfo().clearCorrelatedColumns();
	}
	
	private static ProjectingStatement merge(SchemaContext sc, DMLStatement enclosing, TableInstance ti, PEViewTable theView, ProjectingStatement remapped) {
		if (!(enclosing instanceof SelectStatement))
			return remapped;
		// in particular, it will be hard to yank in filters onto unions, so ignore it for now
		if (!(remapped instanceof SelectStatement))
			return remapped;
		// we always do the pushdown bit		
		SelectStatement ss = (SelectStatement) enclosing;
		Map<PEColumn,ExpressionNode> viewColumnDefinitions = getBackingDefs(sc,theView,(SelectStatement)remapped);
		
		// we shouldn't try swapping the view in if it has more than one ftr - not clear that will behave correctly
		if (remapped.getTablesEdge().size() > 1)
			return pullInFilters(sc,ss,(SelectStatement)remapped,ti,theView,viewColumnDefinitions);
		
		int anchor = -1;
		for(int i = 0; i < ss.getTablesEdge().size(); i++) {
			FromTableReference ftr = ss.getTablesEdge().get(i);
			if (ftr.getBaseTable() != null && ftr.getBaseTable() == ti) {
				anchor = i;
				break;
			}
		}
		if (anchor > -1)
			return merge(sc,ss,ti,theView,(SelectStatement)remapped,anchor, viewColumnDefinitions);
		return pullInFilters(sc,ss,(SelectStatement)remapped,ti,theView,viewColumnDefinitions);
	}
	
	private static SelectStatement pullInFilters(SchemaContext sc, SelectStatement enclosing, SelectStatement def, 
			TableInstance ti, PEViewTable vt,
			Map<PEColumn,ExpressionNode> viewColumnDefs) {
		ExpressionNode wc = enclosing.getWhereClause();
		if (wc == null) return def;
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(wc);
		final TableKey vk = ti.getTableKey();
		List<ExpressionNode> pushDown = new ArrayList<ExpressionNode>();
		for(ExpressionNode en : decompAnd) {
			ListSet<ColumnInstance> cols = ColumnInstanceCollector.getColumnInstances(en);
			ListSet<ColumnInstance> vki = new ListSet<ColumnInstance>();
			ListSet<ColumnInstance> others = new ListSet<ColumnInstance>();
			for(ColumnInstance ci : cols) {
				if (ci.getTableInstance().getTableKey().equals(vk)) {
					vki.add(ci);
				} else {
					others.add(ci);
				}
			}
			if (!others.isEmpty()) continue;
			// only simple clauses
			if (vki.isEmpty()) continue;
			// vki not empty en can be remapped into the view query
			pushDown.add(en);
		}
		// for each pushDown query - we're going to map the view column into the underlying
		if (pushDown.isEmpty())
			return def;
		SelectStatement copy = CopyVisitor.copy(def);
		// figure out how the view def cols map to the underlying cols
		List<ExpressionNode> mapped = new ArrayList<ExpressionNode>();
		for(ExpressionNode en : pushDown) {
			ExpressionNode encopy = (ExpressionNode) en.copy(null);
			ListSet<ColumnInstance> encols = ColumnInstanceCollector.getColumnInstances(encopy);
			for(ColumnInstance ci : encols) 
				mapColumnDef(ci,viewColumnDefs);
			mapped.add(encopy);
		}
		decompAnd = ExpressionUtils.decomposeAndClause(copy.getWhereClause());
		decompAnd.addAll(mapped);
		copy.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
		return copy;
	}

	private static Map<PEColumn,ExpressionNode> getBackingDefs(SchemaContext sc, PEViewTable vt, SelectStatement def) {		
		HashMap<PEColumn,ExpressionNode> backingDefs = new HashMap<PEColumn,ExpressionNode>();
		List<PEColumn> cols = vt.getColumns(sc);
		for(int i = 0; i < cols.size(); i++) 
			backingDefs.put(cols.get(i),ExpressionUtils.getTarget(def.getProjectionEdge().get(i)));
		return backingDefs;
	}

	private static void mapColumnDef(ColumnInstance ci, Map<PEColumn,ExpressionNode> viewColumnDefinitions) {
		PEColumn pec = ci.getPEColumn();
		ExpressionNode coldef = viewColumnDefinitions.get(pec);
		if (coldef == null) return;
		ExpressionNode coldefcopy = (ExpressionNode) coldef.copy(null);
		ci.getParentEdge().set(coldefcopy);
		if (coldefcopy instanceof ColumnInstance) {
			// ok
		} else {
			coldefcopy.setGrouped();
		}		
	}
	
	private static SelectStatement merge(SchemaContext sc,SelectStatement enclosing,TableInstance ti,
			PEViewTable theView,SelectStatement viewDef,
			int anchor, Map<PEColumn,ExpressionNode> viewColumnDefinitions) {
		FromTableReference ftr = enclosing.getTablesEdge().get(anchor);
		// so the view table is referenced as the base table - so we either have
		// select ... from V v where ... or select ... from V v join A a join B b
		// take this ftr and swap out the base table for the view base table and take the existing joins, prepend the
		// joins from the view...
		//
		// viewDef has already been copied/modified - so safe to rip it apart
		FromTableReference viewFTR = viewDef.getTablesEdge().get(0);
		List<JoinedTable> mergedJoins = new ArrayList<JoinedTable>();
		if (!(viewFTR.getTarget() instanceof TableJoin)) {
			mergedJoins.addAll(viewFTR.getTableJoins());			
		}
		mergedJoins.addAll(ftr.getTableJoins());
		FromTableReference nftr = new FromTableReference(viewFTR.getTarget());
		nftr.addJoinedTable(mergedJoins);
		ftr.getParentEdge().set(nftr);
		List<ExpressionNode> encDecomp = ExpressionUtils.decomposeAndClause(enclosing.getWhereClause());
		encDecomp.addAll(ExpressionUtils.decomposeAndClause(viewDef.getWhereClause()));
		enclosing.setWhereClause(ExpressionUtils.safeBuildAnd(encDecomp));
		// if enclosing has no order by clause, we should yank in the one from the view def
		if (viewDef.getOrderBysEdge().has() && !enclosing.getOrderBysEdge().has()) {
			enclosing.setOrderBy(viewDef.getOrderBys());
		}
		// now we just need to swap in view table columns for their backing defs
		ListSet<ColumnInstance> cols = ColumnInstanceCollector.getColumnInstances(enclosing);
		for(ColumnInstance ci : cols) 
			mapColumnDef(ci,viewColumnDefinitions);
		
		// completely consumed
		enclosing.getDerivedInfo().removeLocalTable(theView);
		enclosing.getDerivedInfo().addLocalTables(viewDef.getDerivedInfo().getLocalTableKeys());
		enclosing.getDerivedInfo().addNestedStatements(viewDef.getDerivedInfo().getLocalNestedQueries());
		enclosing.getDerivedInfo().clearCorrelatedColumns();

		return null;
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context) throws PEException {
		if (!applies(context.getContext(),stmt)) {
			return null;
		}
		DMLStatement copy = CopyVisitor.copy(stmt);
		if (emitting()) 
			emit("Before view rewrite: " + copy.getSQL(context.getContext()));
		applyViewRewrites(context.getContext(),copy,this);
		if (emitting())
			emit("After view rewrite: " + copy.getSQL(context.getContext()));
		
		return buildPlan(copy, context, DefaultFeaturePlannerFilter.INSTANCE);
	}


}

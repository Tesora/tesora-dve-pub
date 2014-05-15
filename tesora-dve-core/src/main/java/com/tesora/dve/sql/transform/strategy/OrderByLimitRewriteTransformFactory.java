// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.GroupByRewriteTransformFactory.AliasingEntry;
import com.tesora.dve.sql.transform.strategy.aggregation.ProjectionCharacterization;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * Applies when the query has an order by clause and/or a limit clause.
 * The strategy is:
 * x limit => x limit on sites, redist to y on single site, y limit
 * x order by => x on sites, redist to y on single site, y order by
 * x order by limit => x order by limit on sites, redist to y on single site, y order by limit
 * x offset a limit b => x offset 0 limit a+b on sites, redist to y on single site, y offset a limit b
 * 
 * We also occasionally add in order by pk in order to attempt to emulate native behavior
 * where mysql will return rows in index order
 */
public class OrderByLimitRewriteTransformFactory extends TransformFactory {

	private boolean applies(SchemaContext sc, DMLStatement stmt)
			throws PEException {
		if (EngineConstant.LIMIT.has(stmt))
			return true;
		else if (EngineConstant.ORDERBY.has(stmt)) {
			// for now, don't fire on order by null
			List<LanguageNode> obs = EngineConstant.ORDERBY.getMulti(stmt);
			if (obs.size() == 1) {
				SortingSpecification ss = (SortingSpecification) obs.get(0);
				if (ss.getTarget() instanceof LiteralExpression) {
					LiteralExpression litex = (LiteralExpression) ss.getTarget();
					if (litex.isNullLiteral())
						return false;
				}
			}
			return true;
		} else if (EngineConstant.GROUPBY.has(stmt)) {
			if (stmt instanceof SelectStatement) {
				ProjectionCharacterization pc = ProjectionCharacterization.getProjectionCharacterization((SelectStatement)stmt);
				if (!pc.anyAggFunsNoCount()) {
					stmt.getBlock().store(OrderByLimitRewriteTransformFactory.class,Boolean.TRUE);
					return true;
				}
			}
			return false;
		}
		return false;
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.ORDERBY_LIMIT;
	}

	protected static ListSet<TableKey> projectionContainsPK(SchemaContext sc, DMLStatement in) {
		ListSet<LanguageNode> columns = (ListSet<LanguageNode>) in.getDerivedAttribute(EngineConstant.PROJ_COLUMNS, sc);
		ListSet<TableKey> tableKeys = new ListSet<TableKey>();
		for(LanguageNode t : columns) {
			if (!(t instanceof ColumnInstance))
				continue;
			ColumnInstance ci = (ColumnInstance) t;
			PEColumn c = ci.getPEColumn();
			if (c.isPrimaryKeyPart())
				tableKeys.add(ci.getColumnKey().getTableKey());
		}
		return tableKeys;
	}

	protected static DMLStatement addOrderByPK(SchemaContext sc, SelectStatement in, ListSet<TableKey> tabs, boolean makeCopy, ListSet<ColumnKey> addedCols) {
		for(TableKey tk : tabs) {
			if (tk.getAbstractTable().isView()) continue;
			PEKey pk = tk.getAbstractTable().asTable().getPrimaryKey(sc);
			if (pk == null) continue;
			for(PEColumn c : pk.getColumns(sc)) {
				addedCols.add(new ColumnKey(tk, c));
			}
		}
		for(LanguageNode ln : EngineConstant.ORDERBY.getMulti(in)) {
			SortingSpecification ss = (SortingSpecification) ln;
			// targets are either alias instances or direct targets - check for both cases
			ExpressionNode targ = null;
			if (ss.getTarget() instanceof AliasInstance) {
				AliasInstance ai = (AliasInstance) ss.getTarget();
				targ = ai.getTarget().getTarget();
			} else {
				targ = ss.getTarget();
			}
			if (targ instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) targ;
				addedCols.remove(ci.getColumnKey());
			} 
		}
		if (addedCols.isEmpty())
			return null;
		boolean direction = true;
		SelectStatement orderedChild = (makeCopy ? CopyVisitor.copy(in) : in);
		Map<RewriteKey, ExpressionNode> projMap = ExpressionUtils.buildRewriteMap(orderedChild.getProjection());
		for(ColumnKey ck : addedCols) {
			// the order by planner expects aliases to be used if present.
			// note that we can just search on the column key - no schema mods have been made yet, just identity copies
			ExpressionNode matching = projMap.get(ck);
			ExpressionNode target = null;
			if (matching != null) {
				ExpressionAlias ea = (ExpressionAlias) matching.getParent();
				target = ea.buildAliasInstance();
			} else {
				target = orderedChild.getMapper().copyForward(ck.toInstance());
			}
			SortingSpecification sort = 
				new SortingSpecification(target, direction);
			sort.setOrdering(true);
			EngineConstant.ORDERBY.getEdge(orderedChild).add(sort);
			orderedChild.getMapper().getCopyContext().put(ck, ck);
		}
		return orderedChild;
	}

	public static final UnaryFunction<AliasingEntry<SortingSpecification>,SortingSpecification> buildOrderSortingEntry = new UnaryFunction<AliasingEntry<SortingSpecification>,SortingSpecification>() {

		@Override
		public AliasingEntry<SortingSpecification> evaluate(SortingSpecification object) {
			return new OrderByEntry(object);
		}
		
	};
	
	private static class OrderByEntry extends AliasingEntry<SortingSpecification> {
		
		public OrderByEntry(SortingSpecification orig) {
			super(orig);
		}
		
		@Override
		public SortingSpecification buildNew(ExpressionNode target) {
			return new SortingSpecification(target,getOrigNode().isAscending());
		}

		@Override
		public Edge<SortingSpecification, ExpressionNode> getOrigNodeEdge() {
			return getOrigNode().getTargetEdge();
		}
		
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (!applies(ipc.getContext(),stmt))
			return null;

		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		SelectStatement ss = (SelectStatement) stmt;

		SelectStatement working = ss;
		ListSet<ColumnKey> addedPKOrders = new ListSet<ColumnKey>();		

		boolean emulate = SchemaVariables.emulateMysqlLimit(context.getContext());
		if (emulate) {
			ListSet<TableKey> pks = projectionContainsPK(context.getContext(),working);
			if (!pks.isEmpty()) {
				DMLStatement modified = addOrderByPK(context.getContext(),working,pks,true,addedPKOrders);
				if (modified != null)
					working = (SelectStatement) modified;
			}
		}
		
		if (working == ss) // NOPMD by doug on 15/01/13 4:06 PM
			working = CopyVisitor.copy(ss);
		
		ListSet<AliasingEntry<SortingSpecification>> entries = null;

		boolean requiresAggSite = false;

		boolean addExplicitOrderByOnGroupByCols = ((stmt.getBlock().getFromStorage(OrderByLimitRewriteTransformFactory.class) == null) ? false : (Boolean) stmt.getBlock().getFromStorage(OrderByLimitRewriteTransformFactory.class));
		stmt.getBlock().clearFromStorage(OrderByLimitRewriteTransformFactory.class);
		if (addExplicitOrderByOnGroupByCols) {
			entries = GroupByRewriteTransformFactory.buildEntries(context.getContext(), working, working.getGroupBysEdge(), buildOrderSortingEntry, SortingSpecification.getTarget);
			requiresAggSite = true;
		} else {
			entries = GroupByRewriteTransformFactory.buildEntries(context.getContext(), working, working.getOrderBysEdge(), buildOrderSortingEntry, SortingSpecification.getTarget);
			if (working.getOrderBysEdge().has())
				requiresAggSite = true;
		}
		
		SelectStatement child = CopyVisitor.copy(working);
		
		child.getOrderBysEdge().clear();
		LimitSpecification limitSpec = child.getLimit();
		child.setLimit(null);

		RuntimeLimitSpecification rls = RuntimeLimitSpecification.analyze(context.getContext(), ss, limitSpec);
		PlannerContext childContext = context.withTransform(getFeaturePlannerID());
		if (rls.isInconstantLimit() || requiresAggSite)
			childContext = childContext.withAggSite();
		
		ProjectingFeatureStep subRoot = 
				(ProjectingFeatureStep) buildPlan(child, context.withTransform(getFeaturePlannerID()), DefaultFeaturePlannerFilter.INSTANCE);
		
		if (addExplicitOrderByOnGroupByCols) {
			return redist(context, subRoot, ss, context.getTempGroupManager().getGroup(true),
					new ExecutionCost(false,true,subRoot.getCost()),
					DMLExplainReason.GROUP_BY_NO_ORDER_BY.makeRecord(),
					rls,limitSpec,entries,addedPKOrders);
			
		}
		
		if (subRoot.getCost().isSingleSite() || (rls.getInMemoryLimit() != null)) {
			SelectStatement result = (SelectStatement) subRoot.getPlannedStatement();
			applyMods(true,(SelectStatement)stmt,result,rls,limitSpec,entries, addedPKOrders, subRoot.getCost().isSingleSite());
			if (rls.getInMemoryLimit() != null)
				subRoot.setInMemLimit(rls.getInMemoryLimit());
			return subRoot.withPlanner(this);
		} else {
			return redist(context, subRoot, ss, context.getTempGroupManager().getGroup(true),
					new ExecutionCost(false,true,subRoot.getCost()),
					DMLExplainReason.GROUP_BY_NO_ORDER_BY.makeRecord(),
					rls,limitSpec,entries,addedPKOrders);
		}
		
	}


	private static void applyMods(boolean secondPhase, SelectStatement src, SelectStatement onto, 
			RuntimeLimitSpecification rls, LimitSpecification originalLimit,
			ListSet<AliasingEntry<SortingSpecification>> entries,
			ListSet<ColumnKey> pkOrders,
			boolean singleSite) throws PEException {
		if (rls.isInconstantLimit() && !secondPhase) return;
		// we only apply the sorts to the first step if there's a limit
		if (entries != null && (secondPhase || originalLimit != null)) {
			List<ExpressionNode> removed = applySorts(src, onto, secondPhase, entries, pkOrders, singleSite);
			if (removed != null) {
				// may have to clean up elsewhere
				maybeCleanupGroupBys(removed, onto);
			}
		}
		LimitSpecification any = null;
		if (secondPhase && originalLimit != null)
			any = originalLimit;
		else if (!secondPhase && rls.getIntermediateSpecification() != null)
			any = rls.getIntermediateSpecification();
		if (any != null) {
			LimitSpecification updated = onto.getMapper().copyForward(any);
			onto.setLimit(updated);				
		}			
	}
	
	private static void maybeCleanupGroupBys(List<ExpressionNode> removed, SelectStatement targ) throws PEException {
		for(Iterator<SortingSpecification> iter = targ.getGroupBysEdge().iterator(); iter.hasNext();) {
			SortingSpecification ss = iter.next();
			ExpressionNode en = ss.getTarget();
			if (en instanceof AliasInstance) {
				AliasInstance ai = (AliasInstance) en;
				ExpressionAlias projTarg = ai.getTarget();
				if (removed.contains(projTarg))
					iter.remove();
			}
		}
	}

	private static List<ExpressionNode> applySorts(SelectStatement src, SelectStatement targ, boolean modProjection,
			ListSet<AliasingEntry<SortingSpecification>> entries,
			ListSet<ColumnKey> pkOrders,
			boolean singleSite) throws PEException {
		List<ExpressionNode> removed = buildNewSorts(targ,modProjection,pkOrders,entries,singleSite);
		ArrayList<SortingSpecification> sorts = new ArrayList<SortingSpecification>();
		for(AliasingEntry<SortingSpecification> se : entries) {
			if (se.getNew() != null)
				sorts.add((SortingSpecification)se.getNew());
		}
		pasteNewSorts(sorts,targ);
		return removed;
	}	

	protected static void pasteNewSorts(List<SortingSpecification> ns,
			SelectStatement ontoStatement) {
		ontoStatement.setOrderBy(ns);
	}

	protected static List<ExpressionNode> buildNewSorts(SelectStatement result, boolean modProjection,
			ListSet<ColumnKey> pkorders, 
			ListSet<AliasingEntry<SortingSpecification>> entries,
			boolean singleSite) throws PEException {
		List<ExpressionNode> removed = GroupByRewriteTransformFactory.buildNewSorts(result,entries,modProjection);
		
		// now go through and remove anything that needed according to pk orders
		boolean removePKOrders = false;
		if (pkorders != null && !pkorders.isEmpty() && singleSite) 
			removePKOrders = true;
		if (!removePKOrders)
			return removed;
		for(AliasingEntry<SortingSpecification> se : entries) {
			AliasInstance origTarget = (AliasInstance) se.getOrigNode().getTarget();
			ExpressionNode actual = origTarget.getTarget().getTarget();
			if (actual instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) actual;
				if (pkorders.contains(ci.getColumnKey()))
					se.setNew(null);
			}
		}
		return removed;
	}

	protected FeatureStep redist(PlannerContext pc, ProjectingFeatureStep child, SelectStatement source,
			PEStorageGroup redistTargetGroup, ExecutionCost ec, DMLExplainRecord splain,
			RuntimeLimitSpecification rls, LimitSpecification originalLimit, 
			ListSet<AliasingEntry<SortingSpecification>> entries,
			ListSet<ColumnKey> pkOrders) throws PEException {
		RedistFeatureStep rfs =
				child.redist(pc,
						this,
						new TempTableCreateOptions(Model.STATIC, redistTargetGroup)
							.withRowCount(child.getCost().getRowCount())
							.distributeOn(Collections.<Integer> emptyList()),
						null,
						DMLExplainReason.ORDER_BY.makeRecord());
		SelectStatement cs = (SelectStatement) child.getPlannedStatement();
		applyMods(false,source,cs,rls,originalLimit,entries,pkOrders,child.getCost().isSingleSite());

		ProjectingFeatureStep pfs = rfs.buildNewProjectingStep(pc, this, ec, DMLExplainReason.ORDER_BY.makeRecord());
		pfs.getPlannedStatement().normalize(pc.getContext());
		applyMods(true,source,(SelectStatement)pfs.getPlannedStatement(),rls,originalLimit,entries,pkOrders,true);

		return pfs;		
	}		

	
	
}

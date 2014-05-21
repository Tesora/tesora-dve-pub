// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;



import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * Applies when we there is a group by clause or a distinct set quantifier with no agg funs.
 */
public class GroupByRewriteTransformFactory extends TransformFactory {

	private static final Set<FeaturePlannerIdentifier> aggTransforms = 
			new HashSet<FeaturePlannerIdentifier>(
					Arrays.asList(new FeaturePlannerIdentifier[] { FeaturePlannerIdentifier.GENERIC_AGG })); 

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.GROUP_BY;
	}


	public static boolean isSingleSite(PlannerContext pc, ProjectingFeatureStep child) {
		if (child.getCost().isSingleSite())
			return true;
		final SelectStatement result = (SelectStatement) child.getPlannedStatement();
		DistributionVector broadest = EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(result, pc.getContext());
		return broadest.isBroadcast();
	}

	public static final UnaryFunction<AliasingEntry<SortingSpecification>, SortingSpecification> buildGroupSortingEntry = new UnaryFunction<AliasingEntry<SortingSpecification>, SortingSpecification>() {

		@Override
		public AliasingEntry<SortingSpecification> evaluate(SortingSpecification object) {
			return new GroupBySortingEntry(object);
		}

	};

	public static class GroupBySortingEntry extends AliasingEntry<SortingSpecification> {

		public GroupBySortingEntry(SortingSpecification ss) {
			super(ss);
		}

		@Override
		public SortingSpecification buildNew(ExpressionNode target) {
			SortingSpecification ss = new SortingSpecification(target,getOrigNode().isAscending());
			ss.setOrdering(Boolean.FALSE);
			return ss;
		}

		@Override
		public Edge<SortingSpecification, ExpressionNode> getOrigNodeEdge() {
			return getOrigNode().getTargetEdge();
		}
	}

	@Override
	public FeatureStep plan(DMLStatement statement, PlannerContext ipc) throws PEException {
		// we don't match if any previous transform is an agg transform
		for(FeaturePlannerIdentifier ti : aggTransforms) {
			if (ipc.getApplied().contains(ti))
				return null;
		}
		if (EngineConstant.GROUPBY.has(statement)) {
			// ok
		} else if (statement instanceof SelectStatement) {
			SelectStatement ss = (SelectStatement) statement;
			if (ss.getSetQuantifier() != SetQuantifier.DISTINCT)
				return null;
		} else {
			return null;
		}

		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		SelectStatement in = (SelectStatement) statement;
		SelectStatement ss = CopyVisitor.copy(in);

		// if this is a select distinct, add the group by on all of the columns
		if (in.getSetQuantifier() == SetQuantifier.DISTINCT) {
			ListSet<RewriteKey> existingGroupBys = new ListSet<RewriteKey>();
			for(SortingSpecification gb : ss.getGroupBys()) {
				ExpressionNode gbe = null;
				if (gb.getTarget() instanceof AliasInstance) {
					AliasInstance ai = (AliasInstance) gb.getTarget();
					gbe = ai.getTarget();
				} else {
					gbe = gb.getTarget();
				}
				if (gbe instanceof ExpressionAlias) {
					gbe = ((ExpressionAlias)gbe).getTarget();
				}
				existingGroupBys.add(gbe.getRewriteKey());
			}
			for(ExpressionNode en : ss.getProjection()) {
				ExpressionAlias ea = (ExpressionAlias) en;
				ExpressionNode t = ea.getTarget();
				if (!existingGroupBys.contains(t.getRewriteKey())) {
					SortingSpecification ngb = new SortingSpecification(ea.buildAliasInstance(),true);
					ngb.setOrdering(Boolean.FALSE);
					ss.getGroupBysEdge().add(ngb);
				}
			}
		}

		ListSet<AliasingEntry<SortingSpecification>> entries = 
				buildEntries(context.getContext(), ss, ss.getGroupBysEdge(), buildGroupSortingEntry, SortingSpecification.getTarget);

		SelectStatement nss = CopyVisitor.copy(ss);
		MultiEdge<?, SortingSpecification> nssVersion = nss.getGroupBysEdge();
		nssVersion.clear();

		ProjectingFeatureStep child = 
				(ProjectingFeatureStep) buildPlan(nss,context.withTransform(getFeaturePlannerID()), DefaultFeaturePlannerFilter.INSTANCE);

		if (isModifiable(context, child, entries, AliasingEntry.offsetOperator)) {
			// modify, but do not create new plan
			modify(context,in,child,entries,in.getSetQuantifier() == SetQuantifier.DISTINCT);

			return child;
		} else {
			PEStorageGroup targ = context.getTempGroupManager().getGroup(child.getCost().getGroupScore());
			FeatureStep myRoot = redist(context,in,child,entries,targ,new ExecutionCost(false,targ.isSingleSiteGroup(),child.getCost()),
					DMLExplainReason.WRONG_DISTRIBUTION.makeRecord());

			return myRoot;
		}

	}

	@SuppressWarnings("rawtypes")
	public static boolean isModifiable(PlannerContext pc, ProjectingFeatureStep childStep, 
			ListSet<AliasingEntry<SortingSpecification>> entries, 
			UnaryFunction<Integer, AliasingEntry> offsetoperator) {
		if (isSingleSite(pc, childStep))
			return true;
		final SelectStatement result = (SelectStatement) childStep.getPlannedStatement();
		List<ExpressionNode> mappedInstances = new ArrayList<ExpressionNode>();
		for(AliasingEntry se : entries) {
			int offset = offsetoperator.evaluate(se);
			ExpressionAlias ea = (ExpressionAlias) result.getProjectionEdge().get(offset);
			mappedInstances.add(ea.getTarget());
		}			
		List<Column<?>> mapped = new ArrayList<Column<?>>();
		boolean anyFuns = false;
		for(ExpressionNode e : mappedInstances) {
			RewriteKey rk = e.getRewriteKey();
			if (rk instanceof ColumnKey) {
				mapped.add(((ColumnKey)rk).getColumn());
			} else {
				anyFuns = true;
				break;
			}
		}
		if (anyFuns)
			return false;
		return isModifiable(pc, childStep, mapped);
	}

	public static boolean isModifiable(PlannerContext pc, ProjectingFeatureStep child, List<Column<?>> requestedSorting) {
		final SelectStatement result = (SelectStatement) child.getPlannedStatement();
		ListSet<DistributionVector> distOn = EngineConstant.DISTRIBUTION_VECTORS.getValue(result,pc.getContext());
		// we must redist if no dv matches the mapped columns in the same order 
		boolean any = false;
		for(DistributionVector dv : distOn) {
			List<PEColumn> dvect = dv.getColumns(pc.getContext());
			if (dvect.size() != requestedSorting.size())
				continue;
			Iterator<PEColumn> dviter = dvect.iterator();
			Iterator<Column<?>> mviter = requestedSorting.iterator();
			boolean matching = true;
			while(matching && dviter.hasNext() && mviter.hasNext()) {
				PEColumn dvc = dviter.next();
				Column<?> mvc = mviter.next();
				if (dvc != mvc) {
					matching = false;
				}
			}
			if (matching) {
				any = true;
				break;
			}
		}
		return any;
	}

	private static void modify(PlannerContext context, SelectStatement source, 
			ProjectingFeatureStep child,
			ListSet<AliasingEntry<SortingSpecification>> entries,
			boolean distinct) throws PEException {
		SelectStatement cs = (SelectStatement) child.getPlannedStatement();
		applySorts(source,cs,true,entries,distinct);
	}

	protected static List<ExpressionNode> applySorts(SelectStatement src, SelectStatement targ, boolean modProjection,
			ListSet<AliasingEntry<SortingSpecification>> entries,
			boolean distinct) throws PEException {
		List<ExpressionNode> removed = buildNewSorts(targ,entries,modProjection);
		ArrayList<SortingSpecification> sorts = new ArrayList<SortingSpecification>();
		for(AliasingEntry<SortingSpecification> se : entries) {
			if (se.getNew() != null)
				sorts.add((SortingSpecification)se.getNew());
		}
		pasteNewSorts(sorts,targ, distinct);
		return removed;
	}

	protected static void pasteNewSorts(List<SortingSpecification> ns,
			SelectStatement ontoStatement,
			boolean distinct) {
		ontoStatement.setGroupBy(ns);
		if (distinct)
			ontoStatement.setSetQuantifier(SetQuantifier.DISTINCT);
	}

	protected ProjectingFeatureStep redist(PlannerContext pc, SelectStatement source, ProjectingFeatureStep childRoot,
			ListSet<AliasingEntry<SortingSpecification>> entries,
			PEStorageGroup redistTargetGroup, ExecutionCost ec, DMLExplainRecord splain) throws PEException {		
		List<Integer> distKey = new ArrayList<Integer>();
		for(AliasingEntry<SortingSpecification> se : entries) {
			distKey.add(se.getOffset());
		}

		ProjectingFeatureStep redistributed =
				childRoot.redist(pc,
						this,
						new TempTableCreateOptions(Model.STATIC,redistTargetGroup)
				.distributeOn(distKey).withRowCount(ec.getRowCount()),
				null, 
				splain)
				.buildNewProjectingStep(pc, this, ec, splain);

		SelectStatement intent = (SelectStatement) redistributed.getPlannedStatement();
		intent.normalize(pc.getContext());		
		applySorts(source, intent, true, entries, (source.getSetQuantifier() == SetQuantifier.DISTINCT));

		return redistributed;
	}

	public static <T extends LanguageNode, B extends AliasingEntry<T>> ListSet<AliasingEntry<T>> 
	buildEntries(SchemaContext sc, 
			SelectStatement ss, 
			Edge<?, T> sorts,
			UnaryFunction<B,T> entryBuilder,
			UnaryFunction<ExpressionNode, T> expressionAccessor) {

		Map<RewriteKey,ExpressionNode> projEntryByKey = ExpressionUtils.buildRewriteMap(ss.getProjection());

		ListSet<AliasingEntry<T>> entries = new ListSet<AliasingEntry<T>>();

		for(T ob : sorts.getMulti()) {
			AliasingEntry<T> obe = entryBuilder.evaluate(ob); 
			entries.add(obe);
			ExpressionNode targ = expressionAccessor.evaluate(ob); 
			ExpressionNode projTarg = null;
			if (targ instanceof AliasInstance) {
				AliasInstance ai = (AliasInstance) targ;
				projTarg = ai.getTarget();
			} else {
				projTarg = projEntryByKey.get(targ.getRewriteKey());
				if (projTarg == null) 
					obe.setOrigExpr(targ);
			}
			if (projTarg != null) {
				int offset = -1;
				RewriteKey targKey = projTarg.getRewriteKey();
				for(int i = 0; i < ss.getProjectionEdge().size(); i++) {
					ExpressionNode ith = ss.getProjectionEdge().get(i);
					if (targKey.equals(ith.getRewriteKey())) {
						offset = i;
						break;
					}
				}
				if (offset == -1)
					throw new SchemaException(Pass.PLANNER,"Unable to find order by target amongst projection");
				obe.setOffset(offset);
			}
		}

		boolean needsNormalization = false;

		// now, for any obe for which there is no offset, but there is an orig expr, add the orig expr to the
		// projection and record the offset
		for(AliasingEntry<T> obe : entries) {
			if (obe.getOriginalExpression() != null) {
				// add it
				ss.getProjectionEdge().add(obe.getOriginalExpression());
				// get the offset
				obe.setOffset(ss.getProjectionEdge().size() - 1);
				needsNormalization = true;	
			}
		}

		if (needsNormalization) {
			ss.normalize(sc);
			// rebuild the map
			projEntryByKey = ExpressionUtils.buildRewriteMap(ss.getProjection());
		}

		// now, for sorting entries that added synthetic columns, go ahead and build a new alias instance for them
		for(AliasingEntry<T> se : entries) {
			ExpressionAlias ea = (ExpressionAlias) ss.getProjectionEdge().get(se.getOffset());
			if (se.getOriginalExpression() != null) {
				// have to rewrite the order by to use an alias now
				se.getOrigNodeEdge().set(ea.buildAliasInstance());				
			}
		}

		return entries;
	}

	public static <T extends LanguageNode> List<ExpressionNode> buildNewSorts(SelectStatement result, ListSet<AliasingEntry<T>> sorts, boolean modProjection) {
		int projRemove = 0;

		for(AliasingEntry<T> obe : sorts) {
			if (obe.getOriginalExpression() != null)
				projRemove++;
			ExpressionNode sortTarget = null;
			ExpressionNode en = result.getProjectionEdge().get(obe.getOffset());
			if (obe.getOriginalExpression() != null) {
				// we're going to remove this projection entry, so we're going to use the target of the expression alias
				// as the target of the order by
				sortTarget = ExpressionUtils.getTarget(en);
			} else {
				// build an alias instance for the new order by
				if (en instanceof ExpressionAlias) 
					sortTarget = ((ExpressionAlias)en).buildAliasInstance();
				else
					sortTarget = en;
			}
			obe.setNew(obe.buildNew(sortTarget));
		}

		ArrayList<ExpressionNode> removed = null;
		if (modProjection) {
			removed = new ArrayList<ExpressionNode>();
			for(int i = 0; i < projRemove; i++) {
				ExpressionNode en = result.getProjectionEdge().get(result.getProjectionEdge().size() - 1);
				removed.add(en);
				result.getProjectionEdge().remove(result.getProjectionEdge().size() - 1);
			}
		}

		return removed;
	}


	public static abstract class AliasingEntry<T extends LanguageNode> {

		protected T orig;
		protected T repl;
		protected ExpressionNode origTarget;
		protected int offset;

		public AliasingEntry(T orig) {
			this.orig = orig;
			origTarget = null;
			offset = -1;
		}

		public T getOrigNode() {
			return orig;
		}

		public abstract Edge<?,ExpressionNode> getOrigNodeEdge();

		public void setOrigExpr(ExpressionNode en) {
			origTarget = en;
		}

		public void setOffset(int o) {
			offset = o;
		}

		public ExpressionNode getOriginalExpression() {
			return origTarget;
		}

		public int getOffset() {
			return offset;
		}

		public abstract T buildNew(ExpressionNode target);

		public void setNew(T ss) {
			repl = ss;
		}

		public T getNew() {
			return repl;
		}

		@SuppressWarnings("rawtypes")
		public static final UnaryFunction<Integer, AliasingEntry> offsetOperator = new UnaryFunction<Integer, AliasingEntry>() {

			@Override
			public Integer evaluate(AliasingEntry object) {
				return object.getOffset();
			}

		};

	}




}

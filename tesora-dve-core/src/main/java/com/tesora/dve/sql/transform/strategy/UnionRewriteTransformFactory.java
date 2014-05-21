// OS_STATUS: public
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.ComplexFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * Applies to union statements.
 * The strategy is roughly:
 * [1] union all - return all rows on both sides
 * [2] union distinct - distinct both sides, distinct the whole
 * [3] union with a limit and/or order by - push the order by and/or limit down to the branches
 *     and apply to the result
 */
public class UnionRewriteTransformFactory extends TransformFactory {

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.UNION;
	}

	private static List<SortingSpecification> copySorts(List<SortingSpecification> in, final CopyContext cc) {
		return Functional.apply(in, new UnaryFunction<SortingSpecification,SortingSpecification>() {

			@Override
			public SortingSpecification evaluate(SortingSpecification object) {
				return CopyVisitor.copy(object, cc);
			}
			
		});
		
	}
		
	// the given statement should be a copy - this is destructive
	private static UnionStructure buildUnionStructure(ProjectingStatement ps) {
		if (ps instanceof SelectStatement) {
			return new TerminalUnionStructure((SelectStatement)ps);
		} else {
			UnionStatement us = (UnionStatement) ps;
			return new CompoundUnionStructure(us.isUnionAll(),buildUnionStructure(us.getFromEdge().get()),buildUnionStructure(us.getToEdge().get()));
		}
	}
	
	// holds structure independent of the statements
	private abstract static class UnionStructure {

		public abstract boolean isTerminal();
		
		public abstract boolean analyze(List<TerminalUnionStructure> naturalOrder);

		public abstract ProjectingStatement rebuild();
	}
	
	private static class TerminalUnionStructure extends UnionStructure {

		private SelectStatement original;
		private SelectStatement replacement;
		
		public TerminalUnionStructure(SelectStatement orig) {
			original = orig;
		}
		
		public void setReplacement(SelectStatement ss) {
			replacement = ss;
		}
		
		public ProjectingStatement rebuild() {
			return replacement;
		}
		
		public boolean isTerminal() {
			return true;
		}
		
		public boolean analyze(List<TerminalUnionStructure> acc) {
			acc.add(this);
			return false;
		}
		
		public SelectStatement getTerminal() {
			return original;
		}
	}
	
	private static class CompoundUnionStructure extends UnionStructure {
		
		private UnionStructure lhs;
		private UnionStructure rhs;
		private boolean all;
		
		public CompoundUnionStructure(boolean all, UnionStructure lhs, UnionStructure rhs) {
			this.all = all;
			this.lhs = lhs;
			this.rhs = rhs;
		}
		
		public boolean isTerminal() {
			return false;
		}

		public boolean analyze(List<TerminalUnionStructure> acc) {
			boolean lres = lhs.analyze(acc);
			boolean rres = rhs.analyze(acc);
			if (lres || rres)
				return true;
			return !all;
		}
		
		public ProjectingStatement rebuild() {
			ProjectingStatement lps = lhs.rebuild();
			ProjectingStatement rps = rhs.rebuild();
			UnionStatement us = new UnionStatement(lps, rps, all, null);
			us.getDerivedInfo().addNestedStatements(Arrays.asList(new ProjectingStatement[] { lps, rps }));
			return us;
		}

	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (!(stmt instanceof UnionStatement))
			return null;
		
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		UnionStatement us = (UnionStatement) stmt;

		UnionStatement copy = CopyVisitor.copy(us);
		UnionStructure structure = buildUnionStructure(copy);
		List<TerminalUnionStructure> terminals = new ArrayList<TerminalUnionStructure>();
		boolean anyDistinct = structure.analyze(terminals);
		
		List<SortingSpecification> sorts = copy.getOrderBys();
		LimitSpecification limit = copy.getLimit();
		
		CopyContext cc = copy.getMapper().getCopyContext();
		
		if (limit != null) {
			// modify the terminals
			for(TerminalUnionStructure tus : terminals) {
				SelectStatement ss = tus.getTerminal();
				ss.setLimit(CopyVisitor.copy(limit,cc));
				ss.setOrderBy(copySorts(sorts,cc));
			}
		}
		
		Set<FeaturePlannerIdentifier> metoo = Collections.singleton(getFeaturePlannerID());

		PlannerContext childContext = context.withTransform(getFeaturePlannerID());
		if (us.getOrderBysEdge().has() || us.getLimitEdge().has())
			childContext = childContext.withAggSite();
		
		HashMap<TerminalUnionStructure, FeatureStep> subPlans = 
				new HashMap<TerminalUnionStructure, FeatureStep>();
		for(TerminalUnionStructure tus : terminals)
			subPlans.put(tus, 
					buildPlan(tus.getTerminal(),childContext,new ComplexFeaturePlannerFilter(Collections.<FeaturePlannerIdentifier> emptySet(),metoo)));
		

		// if any set quantifier is distinct - redist all child plans to an agg site and execute there
		ListSet<PEStorageGroup> groups = new ListSet<PEStorageGroup>();
		long rowcount = 0;
		for(TerminalUnionStructure tus : terminals) {
			ProjectingFeatureStep fp = (ProjectingFeatureStep) subPlans.get(tus);
			groups.add(fp.getSourceGroup());
			if (fp.getCost().getRowCount() > -1)
				rowcount += fp.getCost().getRowCount();
		}
		
		Database<?> effectiveDB = subPlans.get(terminals.get(0)).getDatabase(context); 
		PEStorageGroup firstStepGroup = null;

		boolean hasLimit = us.getLimitEdge().has();
		boolean hasOrderBy = us.getOrderBysEdge().has();

		boolean redist = false;
		
		if (anyDistinct || hasLimit || groups.size() > 1) {
			redist = true;
			firstStepGroup = context.getTempGroupManager().getGroup(true);
			TempTableCreateOptions opts = new TempTableCreateOptions(Model.STATIC, firstStepGroup)
				.withRowCount(rowcount);
			for(TerminalUnionStructure tus : terminals) {
				ProjectingFeatureStep subfp = (ProjectingFeatureStep) subPlans.get(tus);
				RedistFeatureStep rfs =
						subfp.redist(context, 
								this,
								opts,
								null,
								null);
				// we don't create a new step here because we're going to edit the selects in place
				SelectStatement intent = rfs.getTargetTempTable().buildSelect(context.getContext());
				intent.normalize(context.getContext());
				subPlans.put(tus, rfs);
				tus.setReplacement(intent);
			}
		} else {
			for(TerminalUnionStructure tus : terminals) {
				ProjectingFeatureStep subfp = (ProjectingFeatureStep) subPlans.get(tus);
				ProjectingStatement result = (ProjectingStatement) subfp.getPlannedStatement();
				tus.setReplacement((SelectStatement) result);
			}
			firstStepGroup = groups.get(0);
		}
		UnionStatement rebuilt = (UnionStatement) structure.rebuild();
		rebuilt.setOrderBy(copySorts(us.getOrderBys(), cc));
		if (us.getLimit() != null)
			rebuilt.setLimit(CopyVisitor.copy(us.getLimit(),cc));
		// we have to build a new projecting step for rebuilt now to thread it in
		// all of the subplans are parallel children
		ProjectingFeatureStep root = 
				DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(context,
						this,
						rebuilt,
						new ExecutionCost(false,true,null,rowcount),
						firstStepGroup,
						effectiveDB,
						null, // vector
						null, // distkey
						DMLExplainReason.UNION.makeRecord()); // explain
		
		if (redist) {
			for(TerminalUnionStructure tus : terminals)
				root.addChild(subPlans.get(tus));
		} else {
			// not redisting, if any of the terminals has children we need to line them all up as prereqs on root			
			for(TerminalUnionStructure tus : terminals) {
				FeatureStep fs = subPlans.get(tus);
				if (!fs.getAllChildren().isEmpty()) {
					root.getPreChildren().addAll(fs.getAllChildren());
				}
			}
		}
		
		root.withParallelChildren();
		
		FeatureStep out = null;
		
		if (hasOrderBy && !redist) {
			// if there was limit we would have taken the first branch; but if all the branches ended up on the same storage group
			// and there is a order by we have to do a final redist
			PEStorageGroup aggGroup = context.getTempGroupManager().getGroup(true);
			rebuilt.getOrderBysEdge().clear();
		
			ProjectingFeatureStep intentStep =
					root.redist(context, this,
							new TempTableCreateOptions(Model.STATIC,aggGroup)
								.withRowCount(rowcount),
							null,
							DMLExplainReason.ORDER_BY.makeRecord())
					.buildNewProjectingStep(context, this, 
							new ExecutionCost(false,true,null,rowcount), 
							DMLExplainReason.ORDER_BY.makeRecord());
			SelectStatement ss = (SelectStatement) intentStep.getPlannedStatement();
			ss.normalize(context.getContext());
			ss.setOrderBy(copySorts(us.getOrderBys(), cc));
			out = intentStep;
		} else {
			out = root;
		}

		return out;
	}
}

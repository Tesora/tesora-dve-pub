package com.tesora.dve.sql.transform.strategy.correlated;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatementUtils;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.AggFunCollector;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.ComplexFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.ApplyOption;
import com.tesora.dve.sql.transform.strategy.CollapsingMutator;
import com.tesora.dve.sql.transform.strategy.ColumnMutator;
import com.tesora.dve.sql.transform.strategy.CompoundExpressionColumnMutator;
import com.tesora.dve.sql.transform.strategy.MutatorState;
import com.tesora.dve.sql.transform.strategy.PassThroughMutator;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.ProjectionMutator;
import com.tesora.dve.sql.transform.strategy.RecallingMutator;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.IndexCollector;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

/*
 * Applies for correlated subqueries in the where clause.  The strategy is:
 * [1] Decompose the where clause, excise the filter enclosing the correlated subquery
 * [2] Simplify the outer projection.  Strip off order by, group by, limit.
 * [3] Add the outer correlated columns to the projection if need be.
 * [4] Plan the outer query, redist to temp table OT
 * [5] Build lookup table LT from OT.  
 * [6] Plan the inner query, redist to temp table ST.
 * [7] Join OT & ST on the correlated colummns, apply the original filter,
 *     undo changes to the projection, group by, order by, limit.
 */
public class WhereClauseCorrelatedSubqueryTransformFactory extends
		CorrelatedSubqueryTransformFactory {

	@Override
	protected EdgeTest getMatchLocation() {
		return EngineConstant.WHERECLAUSE;
	}

	@Override
	protected CorrelatedSubquery buildSubquery(SchemaContext sc, ProjectingStatement subq,
			SelectStatement copy, ListSet<ColumnKey> outerColumns) throws PEException {
		List<ExpressionNode> clauses = ExpressionUtils.decomposeAndClause(copy.getWhereClause());
		ExpressionNode container = subq.ifAncestor(clauses);
		if (container == null)
			throw new PEException("Cannot find subquery within where clause");
		ListSet<ColumnKey> containerColumns = new ListSet<ColumnKey>();
		ListSet<ColumnInstance> concols = ColumnInstanceCollector.getColumnInstances(container);
		for(ColumnInstance ci : concols) {
			if (ci.getEnclosing(SelectStatement.class, null) == copy)
				containerColumns.add(ci.getColumnKey());
		}
		int offset = -1;
		for(int i = 0; i < clauses.size(); i++) {
			if (clauses.get(i) == container) {
				offset = i;
				break;
			}
		}
		return new WhereClauseCorrelatedSubquery(sc,(Subquery)subq.getParent(),outerColumns,offset,container,containerColumns);
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.WC_CORSUB;
	}

	private static class WhereClauseCorrelatedSubquery extends CorrelatedSubquery {

		private int origOffset;
		// the original container for the subq.
		// so if the subq is in the expression foo in ( subq ) 
		// then "foo in ( subq )" is the original container
		private ExpressionNode originalContainer;
		// and then foo is a container column
		private ListSet<ColumnKey> containerColumns;
		
		public WhereClauseCorrelatedSubquery(SchemaContext sc, Subquery sq,
				ListSet<ColumnKey> outerColumns, int origOffset, ExpressionNode origContainer,
				ListSet<ColumnKey> containerColumns) {
			super(sc, sq, EngineConstant.WHERECLAUSE, outerColumns);
			this.origOffset = origOffset;
			this.originalContainer = origContainer;
			this.containerColumns = containerColumns;
		}

		@Override
		public SelectStatement pasteTempTable(PlannerContext pc,
				SelectStatement into, RedistFeatureStep tempTableStep) throws PEException {
			TempTable tt = tempTableStep.getTargetTempTable();
			SelectStatement tq = tempTableStep.buildNewSelect(pc);
			SelectStatement fsq = CopyVisitor.copy(into);
			SelectStatement combined = DMLStatementUtils.compose(pc.getContext(),tq,fsq);
			List<ExpressionNode> mappedJoinCondition = new ArrayList<ExpressionNode>();
			IndexCollector ic = new IndexCollector();
			for(Pair<ColumnKey,ColumnKey> p : joinConditions) {
				ColumnInstance lhk = combined.getMapper().copyColumnKeyForward(p.getFirst()).toInstance();
				ColumnInstance rhk = combined.getMapper().copyColumnKeyForward(p.getSecond()).toInstance();
				FunctionCall fc = new FunctionCall(FunctionName.makeEquals(),lhk,rhk);
				fc.setGrouped();
				mappedJoinCondition.add(fc);
				ic.addColumnInstance(lhk);
				ic.addColumnInstance(rhk);
			}
			ic.setIndexes(pc.getContext());
			ExpressionNode jc = ExpressionUtils.safeBuildAnd(mappedJoinCondition);
			FromTableReference lt = combined.getTablesEdge().get(0);
			FromTableReference rt = combined.getTablesEdge().getLast();
			combined.getTablesEdge().removeLast();
			JoinedTable jt = new JoinedTable(rt.getBaseTable(),jc,JoinSpecification.INNER_JOIN);
			lt.addJoinedTable(jt);
			// we also have to apply the original container again on the where clause
			List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(combined.getWhereClause());
			ColumnInstance replcol = new ColumnInstance(tt.getColumns(pc.getContext()).get(0),lt.getBaseTable());
			sq.getParentEdge().set(replcol);
			ExpressionNode container = originalContainer;
			// container is an EXISTS function then it must contain a subquery so it is already handled so don't add to the WHERE clause
			if (originalContainer instanceof FunctionCall) {
				FunctionCall fc = (FunctionCall) originalContainer;
				if (fc.getFunctionName().isExists())
					container = null;
			}
			ExpressionNode mapped = combined.getMapper().copyForward(container);
			decompAnd.add(mapped);
			combined.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
			for(int i = 0; i < tq.getProjectionEdge().size(); i++)
				combined.getProjectionEdge().remove(0);
			
			combined.normalize(pc.getContext());
			return combined;
		}

		
		@Override
		public void removeFromParent(SelectStatement parent) throws PEException {
			List<ExpressionNode> decomp = ExpressionUtils.decomposeAndClause(parent.getWhereClause());
			decomp.remove(origOffset);
			if (decomp.isEmpty())
				parent.setWhereClause(null);
			else
				parent.setWhereClause(ExpressionUtils.safeBuildAnd(decomp));
		}

		@Override
		public int getOffset() {
			return origOffset;
		}

		public ListSet<ColumnKey> getContainerColumns() {
			return containerColumns;
		}

		private static final SimplifyPass[] simplifications = new SimplifyPass[] {
			stripRedundantLimit,
//			forwardDeeplyCorrelated,
			yankDownRestrictions,
			propagateFixedCorrelation
		};
				
		
		@Override
		protected SimplifyPass[] getSimplifications() {
			return simplifications;
		}

		
	}
	
	private static class DelayAggFunProjectionMutator extends ProjectionMutator {

		public DelayAggFunProjectionMutator(SchemaContext sc) {
			super(sc);
		}

		@Override
		public List<ExpressionNode> adapt(MutatorState ms,
				List<ExpressionNode> proj) {
			// if the entry is an agg fun, do the delayed agg fun column mutator, otherwise passthrough
			for(int i = 0; i < proj.size(); i++) {
				ExpressionNode targ = ExpressionUtils.getTarget(proj.get(i));
				ColumnMutator cm = null;
				if (EngineConstant.AGGFUN.has(targ)) {
					FunctionCall fc = (FunctionCall) targ;
					if (fc.getFunctionName().isCount() && fc.getParametersEdge().get(0) instanceof Wildcard) {
						// we're going to say the standin is any unique column that is a base table
						ColumnKey whatever = null;
						for(FromTableReference ftr : ms.getStatement().getTablesEdge()) {
							if (ftr.getBaseTable() != null) {
								PEAbstractTable<?> pet = ftr.getBaseTable().getAbstractTable();
								if (pet.isView()) continue;
								List<PEKey> yuks = pet.asTable().getUniqueKeys(context);
								if (yuks.isEmpty())
									continue;
								whatever = new ColumnKey(ftr.getBaseTable().getTableKey(),yuks.get(0).getColumns(context).get(0));
								break;
							}
						}
						if (whatever == null)
							throw new SchemaException(Pass.PLANNER,"Unable to find good standin for count(*) for where clause correlated subquery planner");
						cm = new CountStarMutator(whatever);
					} else {
						cm = new RecallingMutator();
					}
				} else {
					cm = new PassThroughMutator();
				}
				cm.setBeforeOffset(i);
				columns.add(cm);
			}
			return applyAdapted(proj,ms);
		}
		
	}
	
	private static class CountStarMutator extends ColumnMutator {

		private SetQuantifier quantifier;
		private ColumnKey standin;
		private Wildcard original;
		
		public CountStarMutator(ColumnKey best) {
			super();
			standin = best;
		}
		
		@Override
		public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
			ExpressionNode targ = getProjectionEntry(proj,getBeforeOffset());
			FunctionCall fc = (FunctionCall) targ;
			original = (Wildcard) fc.getParametersEdge().get(0);
			quantifier = fc.getSetQuantifier();
			// we're going to replace the wildcard with the standin
			ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
			out.add(standin.toInstance());
			return out;
		}

		@Override
		public List<ExpressionNode> apply(List<ExpressionNode> proj,
				ApplyOption ignored) {
			FunctionCall fc = new FunctionCall(FunctionName.makeCount(),original);
			fc.setSetQuantifier(quantifier);
			return Collections.singletonList((ExpressionNode)fc);
		}
		
		
	}
	
	private static class ExplodeAggCompoundExpressionsProjectionMutator extends ProjectionMutator {

		public ExplodeAggCompoundExpressionsProjectionMutator(SchemaContext sc) {
			super(sc);
		}

		@Override
		public List<ExpressionNode> adapt(MutatorState ms,
				List<ExpressionNode> proj) {
			for(int i = 0; i < proj.size(); i++) {
				ExpressionNode targ = ExpressionUtils.getTarget(proj.get(i));
				ListSet<FunctionCall> aggFuns = AggFunCollector.collectAggFuns(targ);
				ColumnMutator cm = null;
				if (!aggFuns.isEmpty()) {
					cm = new CompoundExpressionColumnMutator();
				} else {
					cm = new PassThroughMutator();
				}
				cm.setBeforeOffset(i);
				columns.add(cm);
			}
			return applyAdapted(proj,ms);
		}
		
	}

	@Override
	protected FeatureStep buildSteps(PlannerContext pc,
			List<CorrelatedSubquery> subs, SelectStatement origQuery,
			SelectStatement currentCopy) throws PEException {
		ListSet<WhereClauseCorrelatedSubquery> befores = new ListSet<WhereClauseCorrelatedSubquery>();
		ListSet<WhereClauseCorrelatedSubquery> afters = new ListSet<WhereClauseCorrelatedSubquery>();		
		for(CorrelatedSubquery cs : subs) {
			WhereClauseCorrelatedSubquery wc = (WhereClauseCorrelatedSubquery) cs;
			boolean anyNonTemp = false;
			for(ColumnKey ck : wc.getOuterColumns()) {
				if (!ck.getTableKey().getAbstractTable().isTempTable())
					anyNonTemp = true;
			}
			if (anyNonTemp) {
				afters.add(wc);
			} else {
				befores.add(wc);
			}
		}
		
		ListSet<FeatureStep> deps = new ListSet<FeatureStep>();
		
		SelectStatement workingCopy = currentCopy;
		if (!befores.isEmpty()) {
			// for befores we just need to break them out from the the parent, yank the temp table
			// onto the from clause, slap on the group by on the outer join columns, and plan the queries
			// then, we take the result and redist bcast back onto the pg, and rewrite the working copy
			// to reference the temp tables
			HashMap<WhereClauseCorrelatedSubquery,ProjectingFeatureStep> planned = 
					new HashMap<WhereClauseCorrelatedSubquery,ProjectingFeatureStep>();
			for(int i = befores.size() - 1; i > -1; i--) {
				WhereClauseCorrelatedSubquery subq = befores.get(i);
				subq.removeFromParent(workingCopy);
				subq.getSubquery().getStatement().getMapper().getOriginals().add(workingCopy);
				workingCopy.getDerivedInfo().getLocalNestedQueries().remove(subq.getSubquery().getStatement());
				// we need to group the subq on the join columns
				SelectStatement ss = (SelectStatement) subq.getSubquery().getStatement();
				ListSet<TableKey> tempTables = new ListSet<TableKey>();
				for(Pair<ColumnKey,ColumnKey> p : subq.getJoinColumns()) {
					ss.getGroupBysEdge().add(new SortingSpecification(p.getFirst().toInstance(),true));
					ss.getProjectionEdge().add(p.getFirst().toInstance());
					tempTables.add(p.getSecond().getTableKey());
				}
				for(TableKey tk : tempTables) {
					ss.getTablesEdge().add(new FromTableReference(tk.toInstance()));
					ss.getDerivedInfo().addLocalTable(tk);
				}

				ProjectingFeatureStep innerStep =
						(ProjectingFeatureStep) buildPlan(ss,pc,DefaultFeaturePlannerFilter.INSTANCE);
				planned.put(subq,innerStep);				
			}
			// we need to choose a group to put the temp tables on - it should be whatever group the lhs is on
			for(int i = befores.size() - 1; i > -1; i--) {
				WhereClauseCorrelatedSubquery subq = befores.get(i);
				ListSet<PEStorageGroup> groups = new ListSet<PEStorageGroup>();
				for(ColumnKey ck : subq.getOuterColumns()) {
					groups.add(ck.getTableKey().getAbstractTable().getStorageGroup(pc.getContext()));
				}
				PEStorageGroup aTempGroup = null;
				PEStorageGroup persGroup = null;
				for(PEStorageGroup pesg : groups) {
					if (pesg.isTempGroup())
						aTempGroup = pesg;
					else
						persGroup = pesg;
				}
				PEStorageGroup tempTableGroup = (persGroup != null ? persGroup : aTempGroup);
				ProjectingFeatureStep innerStep = planned.get(subq);
				RedistFeatureStep asTempTable =
						innerStep.redist(pc, this,
								new TempTableCreateOptions(Model.BROADCAST,tempTableGroup)
									.withRowCount(innerStep.getCost().getRowCount()),
								null,
								null);
				workingCopy = subq.pasteTempTable(pc, workingCopy, asTempTable);
				deps.add(asTempTable);
			}

			
		}
		
		int originalProjectionSize = -1;
		ListSet<ColumnKey> allOuterColumns = null;
		List<ProjectionMutator> mutators = null;
		MutatorState ms = null;
		
		ProjectingFeatureStep outerStep = null;
		
		if (!afters.isEmpty()) {
			// first setup
			ListSet<ColumnKey> neededOuterColumns = new ListSet<ColumnKey>();
			ListSet<ColumnKey> neededContainerColumns = new ListSet<ColumnKey>();
			for(int i = afters.size() - 1; i > -1; i--) {
				WhereClauseCorrelatedSubquery subq = afters.get(i);
				subq.removeFromParent(workingCopy);
				subq.getSubquery().getStatement().getMapper().getOriginals().add(workingCopy);
				workingCopy.getDerivedInfo().getLocalNestedQueries().remove(subq.getSubquery().getStatement());
				// we need the columns from the outer query to be present in it's planned version so we can build the join
				neededOuterColumns.addAll(subq.getOuterColumns());
				// but we also need the columns from the container of the inner query to be present as well
				// so we can rebuild the where clause
				neededContainerColumns.addAll(subq.getContainerColumns());
			}
			// we need to add outer columns and any container columns
			originalProjectionSize = workingCopy.getProjectionEdge().size();
			// figure out current columns
			ListSet<ColumnKey> existing = new ListSet<ColumnKey>();
			for(ExpressionNode en : workingCopy.getProjectionEdge()) {
				ExpressionNode t = ExpressionUtils.getTarget(en);
				if (t instanceof ColumnInstance) {
					ColumnInstance ci = (ColumnInstance) t;
					existing.add(ci.getColumnKey());
				}
			}
			ListSet<ColumnKey> added = new ListSet<ColumnKey>();
			for(ColumnKey ck : neededOuterColumns) {
				if (existing.add(ck)) {
					added.add(ck);
				}
			}
			for(ColumnKey ck : neededContainerColumns) {
				if (existing.add(ck))
					added.add(ck);
			}
			for(ColumnKey ck : added) {
				workingCopy.getProjectionEdge().add(ck.toInstance());
			}
			allOuterColumns = neededOuterColumns;
			// now we can run the mutators on the projection
			mutators = new ArrayList<ProjectionMutator>();
			// first pass is exploding compound expressions involving agg funs
			mutators.add(new ExplodeAggCompoundExpressionsProjectionMutator(pc.getContext()));
			// second pass is stripping off all agg funs and replacing them with their params
			mutators.add(new DelayAggFunProjectionMutator(pc.getContext()));
			// third pass is removing duplicates
			mutators.add(new CollapsingMutator(pc.getContext()));
			ms = new MutatorState(workingCopy);
			List<ExpressionNode> copyProj = workingCopy.getProjection();
			for(ProjectionMutator pm : mutators) {
				copyProj = pm.adapt(ms, copyProj);
			}
			ms.combine(pc.getContext(),copyProj,false);
			
			if (emitting()) {
				emit("preplan rewrite: " + workingCopy.getSQL(pc.getContext(), "  "));
			}

			outerStep = (ProjectingFeatureStep) buildPlan(workingCopy,pc,DefaultFeaturePlannerFilter.INSTANCE);
			outerStep.prefixChildren(deps);
			deps.clear();
			
			RedistFeatureStep tempTableStep =
					redistToAggSite(pc, outerStep);
			SelectStatement fs = tempTableStep.buildNewSelect(pc);

			RedistFeatureStep lookupTableStep =
					buildLookupTable(pc,tempTableStep,allOuterColumns,origQuery.getSingleGroup(pc.getContext()));			
			TempTable lt = lookupTableStep.getTargetTempTable();
			for(CorrelatedSubquery csq : afters) {
				if (emitting())
					emit("wc subq: " + csq.getSubquery().getStatement().getSQL(pc.getContext(),"  "));
				SelectStatement rewritten = csq.rewriteSubqueryUsingLookupTable(pc.getContext(),lt); 
				ProjectingFeatureStep childStep =
						(ProjectingFeatureStep) buildPlan(rewritten,pc,
					new ComplexFeaturePlannerFilter(Collections.<FeaturePlannerIdentifier> emptySet(),
				TransformFactory.allTransforms));
				childStep.getPreChildren().add(lookupTableStep);
				RedistFeatureStep tt = redistToAggSite(pc,childStep);
				fs = csq.pasteTempTable(pc, fs, tt);
				deps.add(tt);
			}
			// now we have joined in the correlated subquery - we need to undo what we did in the setup
			// so apply the mutators in reverse order, then remove the trailing proj entries down to the original
			// proj size
			List<ExpressionNode> intermediate = fs.getProjection();
			for(int i = mutators.size() - 1; i > -1; i--) {
				intermediate = mutators.get(i).apply(this,ms, intermediate, new ApplyOption(i,mutators.size() - 1));
			}
			fs.setProjection(intermediate);
			while(fs.getProjectionEdge().size() > originalProjectionSize) {
				fs.getProjectionEdge().remove(fs.getProjectionEdge().size() - 1);
			}
			fs.normalize(pc.getContext());
			workingCopy = fs;
		}
		
		FeatureStep out =
				buildPlan(workingCopy,pc,
						new ComplexFeaturePlannerFilter(Collections.<FeaturePlannerIdentifier> emptySet(), TransformFactory.allTransforms));
		
		out.prefixChildren(deps);
		
		return out;
	}
	
}

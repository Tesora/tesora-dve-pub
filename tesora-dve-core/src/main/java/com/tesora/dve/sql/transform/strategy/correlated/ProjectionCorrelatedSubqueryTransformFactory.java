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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.CaseExpression;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.WhenClause;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.dml.DMLStatementUtils;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.ComplexFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.strategy.ApplyOption;
import com.tesora.dve.sql.transform.strategy.ColumnMutator;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.MutatorState;
import com.tesora.dve.sql.transform.strategy.PassThroughMutator;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.ProjectionMutator;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

/*
 * Handles correlated subqueries on the projection.  The strategy is:
 * [1] Execute the parent query without the correlated subquery, adding the outer correlated columns to the projection if need be.
 * [2] Build a lookup table on the outer correlated columns
 * [3] Execute the inner query as a join against the lookup table, grouped on the inner join columns
 * [4] Redistribute the inner query next to the outer query
 * [5] Execute a left join between the outer query (results from step 1) and the inner query (results from step 4)
 */
public class ProjectionCorrelatedSubqueryTransformFactory extends
		CorrelatedSubqueryTransformFactory {

	@Override
	protected EdgeTest getMatchLocation() {
		return EngineConstant.PROJECTION;
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.PROJ_CORSUB;
	}

	private static Pair<ExpressionNode, Integer> findOffset(SelectStatement copy, ProjectingStatement subq) throws PEException {
		int offset = -1;
		ExpressionNode expr = null;
		int counter = -1;
		for(Iterator<ExpressionNode> iter = copy.getProjectionEdge().iterator(); iter.hasNext();) {
			counter++;
			ExpressionNode en = iter.next();
			if (subq.ifAncestor(Collections.singleton(en)) != null) {
				offset = counter;
				expr = en;
				break;
			}
		}
		if (offset == -1)
			throw new PEException("Cannot find subquery within projection");
		return new Pair<ExpressionNode,Integer>(expr,offset);
	}
	
	@Override
	protected CorrelatedSubquery buildSubquery(SchemaContext sc, ProjectingStatement subq,
			SelectStatement copy, ListSet<ColumnKey> outerColumns) throws PEException {
		Pair<ExpressionNode,Integer> found = findOffset(copy, subq);
		if (subq.getProjections().get(0).size() > 1)
			throw new PEException("Too many columns in correlated subquery");
		return new ProjectionCorrelatedSubquery(sc,(Subquery)subq.getParent(),found.getFirst(),found.getSecond(),outerColumns);
	}
	
	private static class ProjectionCorrelatedSubquery extends CorrelatedSubquery {

		private ExpressionNode origProjEntry;
		private int origProjOffset;

		public ProjectionCorrelatedSubquery(SchemaContext sc, Subquery sq, ExpressionNode projEntry, int projOffset, ListSet<ColumnKey> outerColumns) {
			super(sc, sq,EngineConstant.PROJECTION,outerColumns);
			origProjEntry = projEntry;
			origProjOffset = projOffset;
		}

		// we need to be able to reset these
		void setProjEntry(ExpressionNode en) {
			origProjEntry = en;
		}
		
		void setOrigProjOffset(int i) {
			origProjOffset = i;
		}
		
		// ss is the query against the original parent result; we have to fold in
		// a query against our temp table (left outer join) and fix up the projection appropriately
		@Override
		public SelectStatement pasteTempTable(PlannerContext sc,SelectStatement ss, RedistFeatureStep tempTableStep) throws PEException {
			SelectStatement tq = tempTableStep.buildNewSelect(sc);
			SelectStatement fsq = CopyVisitor.copy(ss);
			SelectStatement combined = DMLStatementUtils.compose(sc.getContext(),fsq,tq);
			for(int i = 0; i < tq.getProjectionEdge().size(); i++)
				combined.getProjectionEdge().remove(combined.getProjectionEdge().size() - 1);
			List<ExpressionNode> mappedJoinCondition = new ArrayList<ExpressionNode>();
			Map<TableKey, Set<PEColumn>> keyMap = new HashMap<TableKey, Set<PEColumn>>();
			// build up the join condition; we will use an loj for this
			for(Pair<ColumnKey,ColumnKey> p : joinConditions) {
				ColumnKey lhk = combined.getMapper().copyColumnKeyForward(p.getFirst());
				ColumnKey rhk = combined.getMapper().copyColumnKeyForward(p.getSecond());
				ColumnInstance lci = lhk.toInstance();
				ColumnInstance rci = rhk.toInstance();
				FunctionCall fc = new FunctionCall(FunctionName.makeEquals(),lci,rci);
				fc.setGrouped();
				mappedJoinCondition.add(fc);
				noteJoinedColumn(keyMap, lci, lhk.getPEColumn());
				noteJoinedColumn(keyMap, rci, rhk.getPEColumn());
			}
			for(TableKey tempTableKey : keyMap.keySet()) {
				((TempTable)(tempTableKey.getAbstractTable())).noteJoinedColumns(sc.getContext(), new ArrayList<PEColumn>(keyMap.get(tempTableKey)));
			}
			ExpressionNode jc = ExpressionUtils.safeBuildAnd(mappedJoinCondition);
			FromTableReference lt = combined.getTablesEdge().get(0);
			FromTableReference rt = combined.getTablesEdge().get(1);
			combined.getTablesEdge().remove(1);
			JoinedTable jt = new JoinedTable(rt.getBaseTable(),jc,JoinSpecification.LEFT_OUTER_JOIN);
			lt.addJoinedTable(jt);

			// so the first column in the temp table select is the actual result column of the subquery -
			// we're going to replace that in the orig proj offset, then map the rest forward
			ExpressionNode rewritten = rewriteSubquerySpot(sc.getContext(),ExpressionUtils.getTarget(tq.getProjectionEdge().get(0)));
			ExpressionNode mappedProj = combined.getMapper().copyForward(rewritten);
			List<ExpressionNode> proj = new ArrayList<ExpressionNode>(combined.getProjection());
			proj.add(origProjOffset, mappedProj);
			combined.setProjection(proj);
			
			return combined;
		}

		
		private void noteJoinedColumn(Map<TableKey, Set<PEColumn>> keyMap, ColumnInstance ci, PEColumn peCol) {
			TableInstance ti = ci.getTableInstance();
			if (ti.getAbstractTable().isTempTable()) {
				if (!keyMap.containsKey(ti.getTableKey())) {
					keyMap.put(ti.getTableKey(), new LinkedHashSet<PEColumn>());
				}
				keyMap.get(ti.getTableKey()).add(peCol);
			}
		}
		
		private ExpressionNode rewriteSubquerySpot(SchemaContext sc, ExpressionNode subqRepl) throws PEException {
			ExpressionPath ep = null;
			if (exists)
				ep = ExpressionPath.build(sq.getParent(), origProjEntry);
			else
				ep = ExpressionPath.build(sq, origProjEntry);
			ExpressionNode targ = ExpressionUtils.getTarget(sq.getStatement().getProjections().get(0).get(0));
			ExpressionNode replacement = null;
			if (sq.getStatement().getLimit() != null && sq.getStatement().getLimit().hasLimitOne(sc)) {
				replacement = subqRepl;
			} else if (targ instanceof ColumnInstance) {
				if (!exists)
					throw new PEException("Likely illegal rewrite - correlated subquery not in exists function but not a function");
				FunctionCall isnot = new FunctionCall(FunctionName.makeIsNot(), subqRepl, LiteralExpression.makeNullLiteral());
				replacement =
						new CaseExpression(null,
								LiteralExpression.makeLongLiteral(0),
								Collections.singletonList(new WhenClause(isnot,LiteralExpression.makeLongLiteral(1),null)),
								null);
				replacement.setGrouped();
			} else {
				FunctionCall fc = (FunctionCall)targ;
				FunctionName fn = fc.getFunctionName();
				if (ep.size() == 1) {
					if (fn.isCount()) {
						LiteralExpression otherValue = LiteralExpression.makeLongLiteral(0);
						replacement = new FunctionCall(FunctionName.makeIfNull(),subqRepl,otherValue);
					} else {
						replacement = subqRepl;
					}
				} else {
					replacement = subqRepl;
				}
			}
			ep.update(origProjEntry, replacement);
			return origProjEntry;
		}

		@Override
		public void removeFromParent(SelectStatement parent) throws PEException {
			parent.getProjectionEdge().remove(getOffset());
		}

		@Override
		public int getOffset() {
			return origProjOffset;
		}

		private static final SimplifyPass[] simplifications =
				new SimplifyPass[] {
			stripRedundantLimit,
//			forwardDeeplyCorrelated
			yankDownRestrictions,
			removeCorrelation
		};
		
		@Override
		protected SimplifyPass[] getSimplifications() {
			return simplifications;
		}



	}

	private static class ExplodeComplexCorrelatedSubqueriesProjectionMutator extends ProjectionMutator {

		// we only care about particular elements in the projection -
		MultiMap<Integer,Subquery> byOffset;
		CopyContext cc;
		
		public ExplodeComplexCorrelatedSubqueriesProjectionMutator(
				SchemaContext sc,
				CopyContext cc,
				MultiMap<Integer,Subquery> offsets) {
			super(sc);
			this.byOffset = offsets;
			this.cc = cc;
		}

		@Override
		public List<ExpressionNode> adapt(MutatorState ms,
				List<ExpressionNode> proj) {
			for(int i = 0; i < proj.size(); i++) {
				ColumnMutator cm = null;
				if (byOffset.containsKey(i)) {
					cm = new ExplodeComplexCorrelatedSubqueriesColumnMutator(byOffset.get(i),cc);
				} else {
					cm = new PassThroughMutator();
				}
				cm.setBeforeOffset(i);
				columns.add(cm);
			}
			return applyAdapted(proj,ms);
		}
		
	}
	
	// basically what we need to remember here is a path for each subq
	// and we need to put each column found in the expression that's not part of the subq into the projection
	private static class ExplodeComplexCorrelatedSubqueriesColumnMutator extends ColumnMutator {

		List<Subquery> theQueries;
		ExpressionNode original;
		protected List<ExpressionPath> paths;
		protected CopyContext cc;
		
		
		public ExplodeComplexCorrelatedSubqueriesColumnMutator(Collection<Subquery> allSubqs, CopyContext cc) {
			super();
			theQueries = Functional.toList(allSubqs);
			paths = new ArrayList<ExpressionPath>();
			this.cc = cc;
		}
		
		@Override
		public List<ExpressionNode> adapt(SchemaContext sc,
				List<ExpressionNode> proj, MutatorState ms) {
			original = getProjectionEntry(proj, getBeforeOffset());
			ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
			ListSet<ColumnInstance> columns = ColumnInstanceCollector.getColumnInstances(original);
			for(ColumnInstance ci : columns) {
				if (ci.ifAncestor(theQueries) != null) {
					// handled by the nested query
					continue;
				}
				paths.add(ExpressionPath.build(ci,original));
				out.add(ci);
			}
			// now add entries for the subqueries
			for(Subquery sq : theQueries) {
				paths.add(ExpressionPath.build(sq,original));
				out.add(sq);
			}
			return out;
		}

		@Override
		public List<ExpressionNode> apply(List<ExpressionNode> proj,
				ApplyOption opts) {
			ExpressionNode repl = (ExpressionNode) original.copy(cc);
			for(int i = 0; i < paths.size(); i++) {
				ExpressionNode updated = getProjectionEntry(proj, getAfterOffsetBegin() + i);
				ExpressionPath path = paths.get(i);
				path.update(repl, updated);
			}
			return Collections.singletonList(repl);
		}
		
	}

	@Override
	protected FeatureStep buildSteps(PlannerContext pc,
			List<CorrelatedSubquery> subs, SelectStatement origQuery,
			SelectStatement copy) throws PEException {
		ListSet<ProjectionCorrelatedSubquery> queries = new ListSet<ProjectionCorrelatedSubquery>();
		for(CorrelatedSubquery cs : subs)
			queries.add((ProjectionCorrelatedSubquery) cs);
		List<ProjectionMutator> mutators = new ArrayList<ProjectionMutator>();
		MutatorState ms = null;
		MultiMap<Integer,Subquery> complex = new MultiMap<Integer,Subquery>();
		boolean anyComplex = false;
		for(ProjectionCorrelatedSubquery psc : queries) {
			Collection<Subquery> any = complex.get(psc.getOffset());
			if (any != null && any.size() > 0) anyComplex = true;
			complex.put(psc.getOffset(), psc.getSubquery());
		}
		if (anyComplex) {
			if (emitting()) {
				emit("Before proj explode: " + copy.getSQL(pc.getContext()));
			}
			// we need to build the mutator and blow out the complex expression
			// this also means we will need to rebuild paths on all of the subqs
			mutators.add(new ExplodeComplexCorrelatedSubqueriesProjectionMutator(pc.getContext(),copy.getMapper().getCopyContext(),complex));
			ms = new MutatorState(copy);
			List<ExpressionNode> copyProj = copy.getProjection();
			for(ProjectionMutator pm : mutators) {
				copyProj = pm.adapt(ms, copyProj);
			}
			ms.combine(pc.getContext(),copyProj,false);
			if (emitting()) {
				emit("After proj explode: " + copy.getSQL(pc.getContext()));
			}
			// ok, copy has been modified.  update the offsets.
			TreeMap<Integer,ProjectionCorrelatedSubquery> byOffset = new TreeMap<Integer,ProjectionCorrelatedSubquery>();
			for(ProjectionCorrelatedSubquery subq : queries) {
				Pair<ExpressionNode, Integer> found = findOffset(copy,subq.getSubquery().getStatement());
				subq.setOrigProjOffset(found.getSecond());
				subq.setProjEntry(found.getFirst());
				byOffset.put(found.getSecond(), subq);
			}
			// finally, we need to reorder them in the list to make the rest of this work correctly
			queries.clear();
			queries.addAll(byOffset.values());
		}
		
		// now traverse the queries in reverse order to remove the subqueries; we're also going 
		// to build the dependency info - we only care about the outer columns that are needed
		// note that corsub impls modify as needed - assume that copy is ready for planning at the end of this.
		ListSet<ColumnKey> neededOuterColumns = new ListSet<ColumnKey>();
		for(int i = queries.size() - 1; i > -1; i--) {
			CorrelatedSubquery sq = queries.get(i);
			// we need to fix the mapper for the nested query for what we need to do later
			sq.getSubquery().getStatement().getMapper().getOriginals().add(copy);
			sq.removeFromParent(copy);
			copy.getDerivedInfo().getLocalNestedQueries().remove(sq.getSubquery().getStatement());
			neededOuterColumns.addAll(sq.getOuterColumns());
		}

		// now figure out which of the outer join needed columns are already on the projection
		ListSet<ColumnKey> allOuterColumns = new ListSet<ColumnKey>();
		allOuterColumns.addAll(neededOuterColumns);
		for(ExpressionNode en : copy.getProjection()) {
			ExpressionNode targ = ExpressionUtils.getTarget(en);
			if (targ instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) targ;
				neededOuterColumns.remove(ci.getColumnKey());
			}
		}

		for(ColumnKey ck : neededOuterColumns) {
			copy.getProjectionEdge().add(ck.toInstance());
		}

		if (copy.getProjectionEdge().size() != origQuery.getProjectionEdge().size())
			copy.normalize(pc.getContext());

		if (emitting())
			emit("out " + copy.getSQL(pc.getContext(), "  "));

		ProjectingFeatureStep childStep =
				(ProjectingFeatureStep) buildPlan(copy,pc,DefaultFeaturePlannerFilter.INSTANCE);
	
		RedistFeatureStep onAggSiteStep = redistToAggSite(pc,childStep);
		TempTable tt = onAggSiteStep.getTargetTempTable();
		SelectStatement fs = tt.buildSelect(pc.getContext());
		RedistFeatureStep lookupTableStep = buildLookupTable(pc,onAggSiteStep,allOuterColumns,origQuery.getSingleGroup(pc.getContext()));
		TempTable lt = lookupTableStep.getTargetTempTable();
		ListSet<FeatureStep> deps = new ListSet<FeatureStep>();
		for(CorrelatedSubquery csq : queries) {
			if (emitting())
				emit("proj subq: " + csq.getSubquery().getStatement().getSQL(pc.getContext(), "  "));
			SelectStatement rewritten = csq.rewriteSubqueryUsingLookupTable(pc.getContext(),lt);
			ProjectingFeatureStep subqStep = (ProjectingFeatureStep) buildPlan(rewritten,pc,
					new ComplexFeaturePlannerFilter(Collections.<FeaturePlannerIdentifier> emptySet(),
							TransformFactory.allTransforms));
			subqStep.getPreChildren().add(lookupTableStep);
			RedistFeatureStep redistToAgg = 
					redistToAggSite(pc,subqStep);
			fs = csq.pasteTempTable(pc, fs, redistToAgg);
			deps.add(redistToAgg);
		}
		for(int i = 0; i < neededOuterColumns.size(); i++) {
			fs.getProjectionEdge().remove(fs.getProjectionEdge().size() - 1);
		}

		// if I have mutators - then I need to unexplode the projection.
		if (ms != null) {
			List<ExpressionNode> intermediate = fs.getProjection();
			for(int i = mutators.size() - 1; i > -1; i--) {
				intermediate = mutators.get(i).apply(this,ms, intermediate, new ApplyOption(i,mutators.size() - 1));
			}
			fs.setProjection(intermediate);
		}

		ProjectingFeatureStep returnStep = 
				DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(
						pc, 
						this, 
						fs, 
						new ExecutionCost(true,true,null,-1),
						pc.getTempGroupManager().getGroup(true),
						origQuery.getDatabase(pc.getContext()),
						EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(fs,pc.getContext()), 
						null,
						DMLExplainReason.PROJECTION_CORRELATED_SUBQUERY.makeRecord());

		returnStep.prefixChildren(deps);
		
		return returnStep;
	}

		
}

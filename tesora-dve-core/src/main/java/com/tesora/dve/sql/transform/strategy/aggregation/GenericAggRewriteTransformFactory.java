package com.tesora.dve.sql.transform.strategy.aggregation;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.transform.TableInstanceCollector;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.strategy.ApplyOption;
import com.tesora.dve.sql.transform.strategy.CollapsingMutator;
import com.tesora.dve.sql.transform.strategy.ColumnMutator;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.GroupByRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.GroupByRewriteTransformFactory.AliasingEntry;
import com.tesora.dve.sql.transform.strategy.MutatorState;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.ProjectionMutator;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.ListSet;

/*
 * Applies if any agg funs are found.
 */
public class GenericAggRewriteTransformFactory  extends TransformFactory {

	private boolean applies(SchemaContext sc, DMLStatement stmt)
			throws PEException {
		if (stmt instanceof SelectStatement) {
			SelectStatement ss = (SelectStatement) stmt;
			ProjectionCharacterization pc = ProjectionCharacterization.getProjectionCharacterization(ss);
			return pc.anyAggFuns();
		}
		return false;
	}
	
	
	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.GENERIC_AGG;
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (!applies(ipc.getContext(),stmt))
			return null;
		
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		SelectStatement ss = (SelectStatement) stmt;
		SelectStatement copy = CopyVisitor.copy(ss);
				
		boolean grand = !copy.getGroupBysEdge().has();
		
		ArrayList<ProjectionMutator> mutators = new ArrayList<ProjectionMutator>();
		mutators.add(new CompoundExpressionProjectionMutator(context.getContext()));
		mutators.add(new AggFunProjectionMutator(context.getContext()));
//		mutators.add(new SimplifyingMutator(ss.getPersistenceContext()));
		mutators.add(new CollapsingMutator(context.getContext()));

		AggregationMutatorState ms = new AggregationMutatorState(context.getContext(),copy);
		List<ExpressionNode> intermediate = copy.getProjection();
		for(ProjectionMutator pm : mutators) {
			intermediate = pm.adapt(ms, intermediate);
		}
		ms.combine(context.getContext(),intermediate,true);
		SelectStatement child = ms.getStatement();

		ProjectingFeatureStep childStep = (ProjectingFeatureStep) 
				buildPlan(child, context,
				DefaultFeaturePlannerFilter.INSTANCE);
		
		AggFunProjectionMutator aggFunMutator = (AggFunProjectionMutator) mutators.get(1);
		List<ColumnMutator> aggColumns = aggFunMutator.getAggColumns();
		int ndistinct = 0;
		for(ColumnMutator cm : aggColumns) {
			AggFunMutator afm = (AggFunMutator) cm;
			if (afm.isDistinct())
				ndistinct++;
		}

		if (mustRedist(context, childStep, grand, ms, mutators)) {
			return redist(context, childStep, grand, ndistinct, ms, mutators);
		}
		
		ProjectingFeatureStep out =
				apply(context,childStep,mutators,ms,mutators.size() - 1, 0, new ApplyOption(1,1));
		out = addDesiredGrouping(context, out,ms,mutators,0);
		out = removeSynthetics(out,ms);
		return out;
	}

	private boolean mustRedist(PlannerContext pc, ProjectingFeatureStep childStep, boolean isGrandAgg,
			AggregationMutatorState state, List<ProjectionMutator> mutators) {
		// if it executes on a single site we don't have to redist - regardless of whether
		// it is a grand agg or not
		if (GroupByRewriteTransformFactory.isSingleSite(pc, childStep))
			return false;
		// if it's a grand agg that executes on more than one site we must redist
		if (isGrandAgg)
			return true;
		// for non grand agg - we have to redist only if the data is not distributed correctly
		SelectStatement result = (SelectStatement) childStep.getPlannedStatement();
		List<Column<?>> requestedGrouping = buildRequestedGrouping(state, mutators, result);
		if (requestedGrouping == null || requestedGrouping.isEmpty())
			return true;			
		return !GroupByRewriteTransformFactory.isModifiable(pc,childStep, requestedGrouping);			
	}

	private List<Column<?>> buildRequestedGrouping(AggregationMutatorState state, List<ProjectionMutator> mutators, SelectStatement onStatement) {
		List<Integer> offsets = new ArrayList<Integer>();
		List<ExpressionNode> resultProjection = onStatement.getProjection();
		ListSet<AliasingEntry<SortingSpecification>> sorts = state.getRequestedGrouping();
		for(AliasingEntry<SortingSpecification> se : sorts) {
			int originalOffset = se.getOffset();
			int actualOffset = mapOffset(mutators, originalOffset, 0, mutators.size() - 1);
			offsets.add(new Integer(actualOffset));
		}
		List<Column<?>> requestedGrouping = new ArrayList<Column<?>>();
		for(int i = 0; i < offsets.size(); i++) {
			ExpressionNode targ = ColumnMutator.getProjectionEntry(resultProjection, offsets.get(i));
			if (targ instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) targ;
				requestedGrouping.add(ci.getColumn());
			} else {
				// requires a redist to get distinct grouping (because a table cannot be distributed on a function)
				return null;
			}
		}
		return requestedGrouping;
	}

	// givenOffset is some column mutator within atMutator's beginOffset; find the endOffset at the endMutator
	private int mapOffset(List<ProjectionMutator> mutators, int givenOffset, int firstMutator, int lastMutator) {
		int workingOffset = givenOffset;
		for(int i = firstMutator; i <= lastMutator && i < mutators.size(); i++) {
			ProjectionMutator pm = mutators.get(i);
			ColumnMutator cm = pm.getMutators().get(workingOffset);
			workingOffset = cm.getAfterOffsetBegin();
		}
		return workingOffset;
	}

	private ProjectingFeatureStep apply(PlannerContext pc,
			ProjectingFeatureStep startAt,
			List<ProjectionMutator> mutators,
			AggregationMutatorState state, 
			int lastMutator, int firstMutator, ApplyOption options) throws PEException {
		SelectStatement result = (SelectStatement) startAt.getPlannedStatement();
		List<ExpressionNode> intermediate = result.getProjection();
		for(int i = lastMutator; i >= firstMutator; i--) {
			final ProjectionMutator pm = mutators.get(i);

			// Apply each mutator's children first so that it can use their results.
			if (options.getCurrentStep() == 1) {
				for (final ColumnMutator cm : pm.getMutators()) {
					if (cm instanceof AggFunMutator) {
						final AggFunMutator parentMutator = (AggFunMutator) cm;
						// Evaluate the children and make their results available in the parent's initial step.
						if (parentMutator.hasChildren()) {
							if (emitting()) {
								emit("planning children");
							}

							// Evaluate and redistribute the child results into temp tables.
							final SchemaContext sc = pc.getContext();

							final SelectStatement childEvaluationStmt = buildChildrenEvaluationStmt(result, parentMutator);
							final List<ColumnInstance> parentGroupCols = new ArrayList<ColumnInstance>();
							if (state.hasGrouping()) {
								final List<ExpressionNode> proj = new ArrayList<ExpressionNode>(childEvaluationStmt.getProjection());
								final List<SortingSpecification> grouping = new ArrayList<SortingSpecification>();
								for (final AliasingEntry<SortingSpecification> e : state.getRequestedGrouping()) {
									final ExpressionNode original = e.getOriginalExpression();
									final ColumnInstance ci = unwindAliases(original);
									proj.add(ci);
									grouping.add(e.buildNew(original));
									parentGroupCols.add(ci);
								}
								childEvaluationStmt.setProjection(proj);
								childEvaluationStmt.setGroupBy(grouping);
								
								final SchemaMapper originalMapper = result.getMapper();
								final SchemaMapper parentToTemp = new SchemaMapper(originalMapper.getOriginals(), childEvaluationStmt, originalMapper.getCopyContext());
								childEvaluationStmt.setMapper(parentToTemp);
							}
							
							childEvaluationStmt.normalize(sc);
							
//							final ProjectingFeatureStep plannedChildEvaluationStep = (ProjectingFeatureStep) TransformFactory.buildPlan(childEvaluationStmt, pc, DefaultFeaturePlannerFilter.INSTANCE);
							final ProjectingFeatureStep plannedChildEvaluationStep = (ProjectingFeatureStep) childEvaluationStmt.plan(sc, sc.getBehaviorConfiguration());
							
							// Redist where the parent's initial step executes.
							final RedistFeatureStep plannedChildRedistStep = plannedChildEvaluationStep.redist(pc,
									this,
									new TempTableCreateOptions(Model.BROADCAST,
											result.getStorageGroup(sc)),
									null,
									null);
							startAt.addChild(plannedChildRedistStep);

							final TempTable childResultsTable = plannedChildRedistStep.getTargetTempTable();
							final SelectStatement childResultsProjectingStmt = childResultsTable.buildSelect(sc);
							childResultsProjectingStmt.normalize(sc);

							final List<ExpressionNode> plannedChildProjection = childResultsProjectingStmt.getProjection();
							final Map<AggFunMutator, ExpressionNode> childExprs = parentMutator.getChildren();

							// Make the results available in the parent's initial statement.
							int projIdx = 0;
							for (final AggFunMutator kid : parentMutator.getChildren().keySet()) {

								// Update projection references available to the parent mutator.
								// Unfold aliases to obtain the actual target column instance.
								final ColumnInstance projEntry = unwindAliases(plannedChildProjection.get(projIdx++));
								childExprs.put(kid, projEntry);
								
								// Append child temp tables to the initial parent's statement.
								final List<FromTableReference> fromTables = new ArrayList<FromTableReference>(result.getTables());
								final TempTableInstance childResultsTableInstance = new TempTableInstance(sc, childResultsTable);
								
								// No grouping => grand aggregation => single row join.
								if (!state.hasGrouping()) {
									fromTables.add(new FromTableReference(childResultsTableInstance));
								} else {
									final List<ExpressionNode> joinConditions = new ArrayList<ExpressionNode>(parentGroupCols.size()); 
									for (final ColumnInstance lhs : parentGroupCols) {
										final ColumnInstance rhs = childResultsProjectingStmt.getMapper().copyForward(lhs);
										final FunctionCall joinOnExpr = new FunctionCall(FunctionName.makeEquals(), lhs, rhs);
										joinConditions.add(joinOnExpr);
									}
									
									final ExpressionNode onClause = ExpressionUtils.safeBuildAnd(joinConditions);
									final JoinedTable jt = new JoinedTable(childResultsTableInstance, onClause, JoinSpecification.INNER_JOIN);
									fromTables.get(fromTables.size() - 1).addJoinedTable(jt);
								}
								result.setTables(fromTables);
							}
						}
					}
				}
			}

			intermediate = pm.apply(this, state, intermediate, options);
		}
		result.setSetQuantifier(null);
		result.setProjection(intermediate);
		result.normalize(pc.getContext());
		if (emitting())
			emit("after apply(" + lastMutator + ", " + firstMutator + ", "+ options + "): " + result.getSQL(pc.getContext()));
		return startAt;
	}
	
	private ColumnInstance unwindAliases(final ExpressionNode original) {
		if (original instanceof ExpressionAlias) {
			return unwindAliases(((ExpressionAlias) original).getTarget());
		}
		return (ColumnInstance) original;
	}

	private SelectStatement buildChildrenEvaluationStmt(final SelectStatement original, final AggFunMutator parentMutator) {
		final List<ExpressionNode> availableExprs = original.getProjection();
		final ExpressionNode target = ColumnMutator.getProjectionEntry(availableExprs, parentMutator.getAfterOffsetBegin());
		final SelectStatement childrenEvaluationStmt = CopyVisitor.copy(original);
		final Set<AggFunMutator> children = parentMutator.getChildren().keySet();
		final List<ExpressionNode> childExprs = new ArrayList<ExpressionNode>(children.size());
		for (final AggFunMutator kid : children) {
			childExprs.add(new FunctionCall(kid.getFunctionName(), (ExpressionNode) target.copy(null)));
		}
		childrenEvaluationStmt.setProjection(childExprs);

		return childrenEvaluationStmt;
	}

	private boolean hasMultipleDistinctInDV(ProjectingFeatureStep currentStep, List<ColumnMutator> aggColumns) {
		int distColCount = 0;
		for(ColumnMutator cm : aggColumns) {
			AggFunMutator afm = (AggFunMutator) cm;
			for(int i = afm.getAfterOffsetBegin(); i < afm.getAfterOffsetEnd(); i++) {
				if (afm.isDistinct()) {
					distColCount++;
				}
			}
		}
		
		return distColCount > 1;
	}

	
	private ProjectingFeatureStep redistAndSelectFromAggSite(PlannerContext pc, 
			ProjectingFeatureStep cc) throws PEException {
		ProjectingFeatureStep out = 
				redistToAggSite(pc, cc).buildNewProjectingStep(pc,
						this, 
						cc.getCost(), 
						DMLExplainReason.AGGREGATION.makeRecord());
		out.getPlannedStatement().normalize(pc.getContext());
		return out;
	}

	private RedistFeatureStep redistToAggSite(PlannerContext pc,
			ProjectingFeatureStep cc) throws PEException {
		return cc.redist(pc,
				this,
				new TempTableCreateOptions(Model.STATIC,
						pc.getTempGroupManager().getGroup(true)),
				null,
				DMLExplainReason.AGGREGATION.makeRecord());
	}

	private ProjectingFeatureStep redistForGrandAggDistinct(PlannerContext pc, ProjectingFeatureStep currentStep,
			List<ProjectionMutator> mutators) throws PEException {
		SelectStatement redistSource = (SelectStatement) currentStep.getPlannedStatement();
		List<Integer> distOff = new ArrayList<Integer>();
		List<Integer> nonDistOff = new ArrayList<Integer>();
		List<ColumnMutator> aggColumns = ((AggFunProjectionMutator)mutators.get(1)).getAggColumns();
		for(ColumnMutator cm : aggColumns) {
			AggFunMutator afm = (AggFunMutator) cm;
			for(int i = afm.getAfterOffsetBegin(); i < afm.getAfterOffsetEnd(); i++) {
				int mapped = mapOffset(mutators,i,2,mutators.size() - 1);
				if (afm.isDistinct())
					distOff.add(mapped);
				else
					nonDistOff.add(mapped);
			}
		}
		ListSet<PEColumn> distCols = new ListSet<PEColumn>();
		for(Integer i : distOff) {
			ExpressionNode en = ExpressionUtils.getTarget(redistSource.getProjectionEdge().get(i));
			if (en instanceof ColumnInstance)
				distCols.add(((ColumnInstance)en).getPEColumn());
		}
		List<PEColumn> nonDistCols = new ListSet<PEColumn>();
		for(Integer i : nonDistOff) {
			ExpressionNode en = ExpressionUtils.getTarget(redistSource.getProjectionEdge().get(i));
			if (en instanceof ColumnInstance)
				nonDistCols.add(((ColumnInstance)en).getPEColumn());
		}
		if (distCols.size() == distOff.size()) {
			Set<DistributionVector> vects = EngineConstant.DISTRIBUTION_VECTORS.getValue(redistSource, pc.getContext());
			for(DistributionVector dv : vects) {
				if (dv.usesColumns(pc.getContext())) {
					ListSet<PEColumn> dvcols = dv.getColumns(pc.getContext());
					if (dvcols.containsAll(distCols) && distCols.containsAll(dvcols)) {
						return currentStep;
					}
				}
			}				
		}
		if (distOff.size() == redistSource.getProjectionEdge().size() && nonDistOff.isEmpty()) {
			redistSource.setSetQuantifier(SetQuantifier.DISTINCT);
		}
		PEStorageGroup pesg = pc.getTempGroupManager().getGroup(currentStep.getCost().getGroupScore());
		ProjectingFeatureStep out =
				currentStep.redist(pc, 
						this, 
						new TempTableCreateOptions(Model.STATIC, pesg).distributeOn(distOff),
						null,
						DMLExplainReason.GRAND_AGG_DISTINCT.makeRecord())
				.buildNewProjectingStep(pc, this, currentStep.getCost(), 
						DMLExplainReason.GRAND_AGG_DISTINCT.makeRecord());
		out.getPlannedStatement().normalize(pc.getContext());
		return out;
	}

	private ProjectingFeatureStep redistForDesiredDistribution(
			PlannerContext pc,
			ProjectingFeatureStep currentStep,
			List<ProjectionMutator> mutators,
			AggregationMutatorState state,
			int lastApplied) throws PEException {
		List<Integer> distKey = new ArrayList<Integer>();
		for(AliasingEntry<SortingSpecification> se : state.getRequestedGrouping()) {
			int offset = se.getOffset();
			offset = mapOffset(mutators,offset,0,lastApplied);
			distKey.add(offset);
		}
		PEStorageGroup targetGroup = pc.getTempGroupManager().getGroup(currentStep.getCost().getGroupScore());
		ProjectingFeatureStep out =
				currentStep.redist(pc,
						this,
						new TempTableCreateOptions(Model.STATIC,targetGroup).distributeOn(distKey)
						.withRowCount(currentStep.getCost().getRowCount()),
						null,
						DMLExplainReason.WRONG_DISTRIBUTION.makeRecord())
				.buildNewProjectingStep(pc, 
						this, 
						(targetGroup.isSingleSiteGroup() ? new ExecutionCost(true,true,currentStep.getCost()) : currentStep.getCost()),
						DMLExplainReason.WRONG_DISTRIBUTION.makeRecord());
		out.getPlannedStatement().normalize(pc.getContext());
		return out;
	}

	private ProjectingFeatureStep addDesiredGrouping(PlannerContext pc, ProjectingFeatureStep currentStep, 
			AggregationMutatorState state,
			List<ProjectionMutator> mutators,
			int lastApplied) throws PEException {
		SelectStatement stmt = (SelectStatement) currentStep.getPlannedStatement();
		for(AliasingEntry<SortingSpecification> se : state.getRequestedGrouping()) {
			int offset = se.getOffset();
			if (lastApplied > 0) 
				offset = mapOffset(mutators,offset,0,lastApplied);
			ExpressionAlias ea = (ExpressionAlias) stmt.getProjectionEdge().get(offset);
			SortingSpecification ss = se.buildNew(ea.buildAliasInstance());
			stmt.getGroupBysEdge().add(ss);
		}
		if (emitting())
			emit("after addDesiredGrouping(" + lastApplied + "): " + stmt.getSQL(pc.getContext()));
		return currentStep;
	}

	private ProjectingFeatureStep removeSynthetics(ProjectingFeatureStep currentStep,
			AggregationMutatorState state) throws PEException {
		SelectStatement stmt = (SelectStatement) currentStep.getPlannedStatement();
		ListSet<ExpressionAlias> toRemove = new ListSet<ExpressionAlias>();
		for(int i = 0; i < state.getRequestedGrouping().size(); i++) {
			AliasingEntry<SortingSpecification> se = state.getRequestedGrouping().get(i);
			if (se.getOriginalExpression() == null)
				continue;
			// we would have already set the appropriate alias instance in the group by, so now just
			// replace the alias instance with the target, and record the target
			SortingSpecification ss = stmt.getGroupBysEdge().get(i);
			AliasInstance ai = (AliasInstance) ss.getTarget();
			ExpressionAlias ea = ai.getTarget();
			ss.getTargetEdge().set(ea.getTarget());
			toRemove.add(ea);
		}
		for(Iterator<ExpressionNode> iter = stmt.getProjectionEdge().iterator(); iter.hasNext();) {
			ExpressionNode en = iter.next();
			if (en instanceof ExpressionAlias && toRemove.contains(en))
				iter.remove();
		}
		return currentStep;
	}

	
	private ProjectingFeatureStep redist(PlannerContext pc, 
			ProjectingFeatureStep current, 
			boolean grand,
			int ndistinct,
			AggregationMutatorState state, 
			List<ProjectionMutator> mutators) throws PEException {
		// step 1 - get the data in the right distribution
		// the right distribution is however it's grouped (if it is grouped)
		// OR on the distinct columns
		AggFunProjectionMutator aggFunMutator = (AggFunProjectionMutator)mutators.get(1);
		ProjectingFeatureStep currentStep = current;
		if (grand) {
			if (ndistinct == 0) {
				// then, apply step1 on the correct distribution, redist to a temp site; this is apply mutators.size() - 1, 2
				currentStep = apply(pc,currentStep,mutators,state,mutators.size() - 1, 1, new ApplyOption(1,2));
				currentStep = redistAndSelectFromAggSite(pc, currentStep);
				// then, apply step 2 on the agg site.  this is apply 1,0
				currentStep = apply(pc,currentStep,mutators,state,1,0,new ApplyOption(2,2));
			} else {
				if (hasMultipleDistinctInDV(currentStep,aggFunMutator.getAggColumns())) {
					currentStep = redistAndSelectFromAggSite(pc,currentStep);
					currentStep = apply(pc,currentStep,mutators,state,mutators.size() - 1, 1, new ApplyOption(1,1));
				} else {
					currentStep = redistForGrandAggDistinct(pc,currentStep,mutators);
					currentStep = apply(pc,currentStep,mutators,state,mutators.size() - 1,1,new ApplyOption(1,2));
					currentStep = redistAndSelectFromAggSite(pc,currentStep);
					// then, apply step 2 on the agg site.  this is apply 1,0
					currentStep = apply(pc,currentStep,mutators,state,1,0,new ApplyOption(2,2));
				}
			}
		} else {
			// if we have distinct columns - we have to redistribute before we start computing
			// but if we don't we can apply the step 1 now
			if (ndistinct == 0) {
				if (aggFunMutator.requiresPlainFirstPass()) {
					// first pass is redist for desired distribution, then apply both steps and add desired grouping
					currentStep = redistForDesiredDistribution(pc,currentStep,mutators,state,mutators.size() - 1) ;
					currentStep = apply(pc,currentStep,mutators,state,mutators.size() - 1, 0, new ApplyOption(1,1));
					currentStep = addDesiredGrouping(pc,currentStep,state,mutators,1);
				} else {
					currentStep = apply(pc,currentStep,mutators,state,mutators.size() - 1,1, new ApplyOption(1,2));
					currentStep = addDesiredGrouping(pc, currentStep, state, mutators, 1);
					currentStep = redistForDesiredDistribution(pc,currentStep,mutators,state,1);
					currentStep = apply(pc,currentStep,mutators,state,1,0,new ApplyOption(2,2));
					currentStep = addDesiredGrouping(pc, currentStep, state, mutators,0);
				}
			} else {
				// if we have distinct columns, we don't apply step 1 at all and go straight to step 2
				currentStep = redistForDesiredDistribution(pc, currentStep, mutators, state,4);
				currentStep = apply(pc, currentStep, mutators, state, mutators.size() - 1, 0, new ApplyOption(1,1));
				currentStep = addDesiredGrouping(pc, currentStep, state, mutators, 0);
			}
			currentStep = removeSynthetics(currentStep, state);
		} 
		
		return currentStep;
	}

	
	
}

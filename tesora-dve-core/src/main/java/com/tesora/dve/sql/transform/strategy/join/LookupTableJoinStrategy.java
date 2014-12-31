package com.tesora.dve.sql.transform.strategy.join;

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


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatementUtils;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.IndexCollector;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.ListSet;

/*
 * The lookup table strategy is between a constrained partition and one that is either lesser constrained
 * or unconstrained.  The strategy is:
 * [1] Redist constrained side to T1 on temp group distributed on join columns
 * [2] Select distinct on join columns on T1 redist bcast to LT on pg
 * [3] Execute inner join between unconstrained side and LT to T2, redist to temp group on join columns
 * [4] Execute T1 join T2 on temp group 
 */
class LookupTableJoinStrategy extends JoinStrategy {

	private StrategyTable constrainedSide;
	
	public LookupTableJoinStrategy(PlannerContext pc, 
			JoinEntry p,
			StrategyTable leftTable,
			StrategyTable rightTable,
			StrategyTable constrained,
			DMLExplainRecord dmle) {
		super(pc, p, leftTable, rightTable, dmle);
		constrainedSide = constrained;
	}
	
	@Override
	public JoinedPartitionEntry build() throws PEException {
		ExecutionCost combinedCost = buildCombinedCost();
		PEStorageGroup tempGroup =  
				getPlannerContext().getTempGroupManager().getGroup(combinedCost.getGroupScore());
		StrategyTable constrained = null;
		StrategyTable unconstrained = null;
		if (constrainedSide == left) {
			constrained = left;
			unconstrained = right;
		} else {
			constrained = right;
			unconstrained = left;
		}
		// we've found the constrained and unconstrained sides - now find the columns in the join
		List<ExpressionNode> constrainedJC = constrained.getEntry().mapDistributedOn(rje.getJoin().getRedistJoinExpressions(constrained.getSingleTable()));
		List<ExpressionNode> unconstrainedJC = unconstrained.getEntry().mapDistributedOn(rje.getJoin().getRedistJoinExpressions(unconstrained.getSingleTable()));			

		// redistribute the constrained side onto the temp group.
		RedistFeatureStep redistConstrained =
				colocateViaRedist(getPlannerContext(),
						getJoin(),
						constrained.getSingleTable(),
						constrained.getEntry(),
						Model.STATIC,
						tempGroup,
						true,
						explain,
						rje.getFeaturePlanner());
		
		// the constrained side is represented by this temp table
		constrained.getEntry().setStep(redistConstrained);
		
		// next, we have to build the select distinct(id) redist'd to the pg
		RedistFeatureStep lookupTableStep =
				buildLookupTableRedist(getPlannerContext(),constrained.getEntry(),
						redistConstrained,rje,unconstrained.getGroup(),true);

		// now we have the unconstrained side still on the pers group
		// and we have a new temp table (lookupTableStep) also on the pers group. 
		// this temp table is the result of the join between the lookup table
		// and the unconstrained side.  the unconstrained entry will be represented by
		// this temp table.
		RedistFeatureStep redistUnconstrained =
				buildLookupJoinRedist(getPlannerContext(),
						lookupTableStep,
						constrainedJC,
						tempGroup,
						rje,
						unconstrainedJC,
						unconstrained.getEntry());

		unconstrained.getEntry().setStep(redistUnconstrained);

		// so now we have redistConstrained and redistUnconstrained, we can build the ipe.
		// the two RedistFeatureSteps end up being the children for the new ipe, but we need to build out the join kernel.
		
		// figure out which is left and right now
		PartitionEntry leftEntry = null;
		PartitionEntry rightEntry = null;
		if (constrainedSide == left) {
			leftEntry = constrained.getEntry();
			rightEntry = unconstrained.getEntry();
		} else {
			leftEntry = unconstrained.getEntry();
			rightEntry = constrained.getEntry();
		}

		RedistFeatureStep leftStep = (RedistFeatureStep) leftEntry.getStep(null);
		RedistFeatureStep rightStep = (RedistFeatureStep) rightEntry.getStep(null);

		return buildResultEntry(
				leftStep.buildNewSelect(getPlannerContext()),
				leftStep.getTargetTempTable().getDistributionVector(getSchemaContext()),
				rightStep.buildNewSelect(getPlannerContext()),
				rightStep.getTargetTempTable().getDistributionVector(getSchemaContext()),
				combinedCost,
				tempGroup,
				false // never parallel because we have an ordering issue
				);
				
	}
	
	// we have the temp table on the temp group for the constrained side
	// we need to build the bcast temp table on the pers group
	public static RedistFeatureStep buildLookupTableRedist(PlannerContext pc, PartitionEntry srcEntry,
			RedistFeatureStep constrainedOnTempGroup,
			JoinEntry rje, PEStorageGroup targetGroup, boolean indexJoinColumns) throws PEException {
		ProjectingFeatureStep selectDistinct = 
				constrainedOnTempGroup.buildNewProjectingStep(pc, rje.getFeaturePlanner(), null, 
						DMLExplainReason.LOOKUP_JOIN_LOOKUP_TABLE.makeRecord());
		SelectStatement ss = (SelectStatement) selectDistinct.getPlannedStatement();
		DistributionVector distVect = constrainedOnTempGroup.getTargetTempTable().getDistributionVector(pc.getContext());

		ListSet<ColumnKey> mappedColumnsInJoin = null;
		if (rje.getJoin().getJoin() != null) {
			ListSet<ColumnKey> columnsInJoin = ColumnInstanceCollector.getColumnKeys(
					ColumnInstanceCollector.getColumnInstances(rje.getJoin().getJoin().getJoinOn()));
			// build the set of columns in the src entry projection
			ListSet<ColumnKey> srcColumns = new ListSet<ColumnKey>();
			for(BufferEntry be : srcEntry.getBufferEntries()) {
				ExpressionNode targ = ExpressionUtils.getTarget(be.getTarget());
				if (targ instanceof ColumnInstance) {
					ColumnInstance ci = (ColumnInstance) targ;
					srcColumns.add(ci.getColumnKey());
				}
			}
			columnsInJoin.retainAll(srcColumns);
			mappedColumnsInJoin = new ListSet<ColumnKey>();
			for(ColumnKey ck : columnsInJoin) {
				mappedColumnsInJoin.add(ss.getMapper().copyColumnKeyForward(ck));
			}
		}
		
		
		for(Iterator<ExpressionNode> iter = ss.getProjectionEdge().iterator(); iter.hasNext();) {
			ExpressionNode en = iter.next();
			if (en instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) en;
				PEColumn pec = ci.getPEColumn();
				if (!distVect.contains(srcEntry.getSchemaContext(),pec) && (mappedColumnsInJoin == null || !mappedColumnsInJoin.contains(ci.getColumnKey())))
					iter.remove();
			}
		}
		ss.setSetQuantifier(SetQuantifier.DISTINCT);
		ss.normalize(srcEntry.getSchemaContext());

		// now that we have the select distinct set up
		// redistribute it bcast back onto the pers group
		RedistFeatureStep out =
				selectDistinct.redist(pc, rje.getFeaturePlanner(),
						new TempTableCreateOptions(Model.BROADCAST,targetGroup),
						null, 
						DMLExplainReason.LOOKUP_JOIN_LOOKUP_TABLE.makeRecord());

		if (indexJoinColumns)
			out.getTargetTempTable().noteJoinedColumns(pc.getContext(), out.getTargetTempTable().getColumns(pc.getContext()));
			
		return out;
	}
	
	public static RedistFeatureStep buildLookupJoinRedist(PlannerContext pc,
			RedistFeatureStep lookupTable,
			List<ExpressionNode> lookupJoinColumns,
			PEStorageGroup targetGroup,
			JoinEntry origEntry,
			List<ExpressionNode> origJoinColumns,
			PartitionEntry nonLookupSide) throws PEException {
		// we will modify the existing non lookup side projecting feature step, and add the lookup table
		// as a requirement to it.  
		ProjectingFeatureStep nonLookupStep = (ProjectingFeatureStep) nonLookupSide.getStep(null);
		nonLookupSide.maybeForceDoublePrecision(nonLookupStep);
		SelectStatement lookupSelect = lookupTable.getTargetTempTable().buildSelect(pc.getContext());
		SelectStatement nonLookupSelect = (SelectStatement) nonLookupStep.getPlannedStatement();

		SelectStatement actualJoinStatement = DMLStatementUtils.compose(pc.getContext(),nonLookupSelect,lookupSelect);			
		List<ExpressionNode> ands = ExpressionUtils.decomposeAndClause(actualJoinStatement.getWhereClause());

		// instead of building the join spec directly, map forward the original join condition from the joined table if it's available
		IndexCollector ic = new IndexCollector();
		if (origEntry.getJoin().getJoin() != null) {
			FunctionCall mapped = (FunctionCall) actualJoinStatement.getMapper().copyForward(origEntry.getJoin().getJoin().getJoinOn());
			ands.add(mapped);
			ListSet<ColumnInstance> cols = ColumnInstanceCollector.getColumnInstances(mapped);
			for(ColumnInstance ci : cols)
				ic.addColumnInstance(ci);
		} else {
			Map<RewriteKey,ExpressionNode> projEntries = null;
			for(int i = 0; i < origJoinColumns.size(); i++) {
				ColumnKey mck = actualJoinStatement.getMapper().mapExpressionToColumn(origJoinColumns.get(i));
				ColumnKey muck = actualJoinStatement.getMapper().mapExpressionToColumn(lookupJoinColumns.get(i));
				ExpressionNode mc = null;
				ExpressionNode muc = null;
				if (mck == null || muck == null) {
					if (projEntries == null) {
						projEntries = new HashMap<RewriteKey,ExpressionNode>();
						for(ExpressionNode en : actualJoinStatement.getProjectionEdge()) {
							ExpressionNode actual = ExpressionUtils.getTarget(en);
							if (actual instanceof ColumnInstance) {
								projEntries.put(((ColumnInstance)actual).getColumnKey(),actual);
							} else {
								projEntries.put(new ExpressionKey(actual), actual);
							}
						}
					}
					if (mck == null) 
						mc = (ExpressionNode) projEntries.get(new ExpressionKey(origJoinColumns.get(i))).copy(null);
					if (muck == null)
						mc = (ExpressionNode) projEntries.get(new ExpressionKey(lookupJoinColumns.get(i))).copy(null);
				}
				if (mc == null)
					mc = mck.toInstance();
				if (muc == null)
					muc = muck.toInstance();
				if (mc instanceof ColumnInstance)
					ic.addColumnInstance((ColumnInstance)mc);
				if (muc instanceof ColumnInstance)
					ic.addColumnInstance((ColumnInstance)muc);
				FunctionCall eq = new FunctionCall(FunctionName.makeEquals(),mc, muc);
				ands.add(eq);
			}
		}
		ic.setIndexes(origEntry.getSchemaContext());

		TempTable lookupEntryTarget = lookupTable.getTargetTempTable();
		
		// everything from the lhs that's in the projection should be cleared - it's invisible
		// note that we do it after building the where clause so that the original join condition can be mapped
		for(Iterator<ExpressionNode> iter = actualJoinStatement.getProjectionEdge().iterator(); iter.hasNext();) {
			ExpressionNode en = iter.next();
			ExpressionNode targ = ExpressionUtils.getTarget(en);
			if (targ instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) targ;
				if (ci.getTableInstance().getAbstractTable() == lookupEntryTarget)
					iter.remove();
			}
		}

		actualJoinStatement.setWhereClause(ExpressionUtils.safeBuildAnd(ands));
		actualJoinStatement.normalize(origEntry.getSchemaContext());

		// build a new projecting feature step
		ProjectingFeatureStep lookupJoinStep =
				DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(origEntry.getPlannerContext(),
						origEntry.getFeaturePlanner(),
						actualJoinStatement,
						new ExecutionCost(false,false,null,-1),
						nonLookupStep.getSourceGroup(),
						actualJoinStatement.getDatabase(origEntry.getSchemaContext()),
						nonLookupStep.getDistributionVector(),
						null, 
						DMLExplainReason.LOOKUP_JOIN.makeRecord());

		// arrange for the children of the nonlookup side to become my children
		// and add the lookup table step as well
		lookupJoinStep.getSelfChildren().addAll(nonLookupStep.getAllChildren());
		lookupJoinStep.getSelfChildren().add(lookupTable);
		// children must be sequential - no need to modify here
		
		List<Integer> mappedRedistOn = nonLookupSide.mapDistributedOn(origJoinColumns, actualJoinStatement);

		// now remove the lookup table from the mapper - have to do this pretty late - at this point
		// the query won't be manipulated any more
		actualJoinStatement.getMapper().remove(lookupEntryTarget);

		RedistFeatureStep out =
				lookupJoinStep.redist(origEntry.getPlannerContext(),
						origEntry.getFeaturePlanner(),
						new TempTableCreateOptions(Model.STATIC,targetGroup)
							.distributeOn(mappedRedistOn).withRowCount(origEntry.getScore().getRowCount()),
						null,
						DMLExplainReason.LOOKUP_JOIN.makeRecord());
		return out;

	}
			
	public static SelectStatement filterEntryProjection(SelectStatement in, PartitionEntry jre) throws PEException {
		PartitionEntry actual = jre.getActualEntry();
		SelectStatement expecting = null;
		if (actual instanceof OriginalPartitionEntry)
			expecting = ((OriginalPartitionEntry)actual).getChildCopy();
		else
			expecting = actual.getJoinQuery(null);
		ListSet<ColumnKey> ec = new ListSet<ColumnKey>();
		for(ExpressionNode en : expecting.getProjection()) {
			ExpressionNode targ = ExpressionUtils.getTarget(en);
			if (targ instanceof ColumnInstance) {
				ColumnKey was = ((ColumnInstance)targ).getColumnKey();
				ColumnKey isnow = in.getMapper().copyColumnKeyForward(was);
				if (isnow == null) {
					throw new SchemaException(Pass.PLANNER, "Lost column during lookup table join");
				}
				ec.add(isnow);
			} else if (targ instanceof FunctionCall) {
				ExpressionNode exn = targ;
				RewriteKey rk = in.getMapper().mapExpressionToColumn(exn);
				while (rk == null && (exn.getParent() instanceof ExpressionNode)) {
					exn = (ExpressionNode)exn.getParent();
					rk = in.getMapper().mapExpressionToColumn(exn);
				}
				if (rk != null) {
					ec.add((ColumnKey) rk);
				}
			}
		}
		for(Iterator<ExpressionNode> iter = in.getProjectionEdge().iterator(); iter.hasNext();) {
			ExpressionNode en = ExpressionUtils.getTarget(iter.next());
			if (en instanceof ColumnInstance) {
				ColumnKey ck = ((ColumnInstance)en).getColumnKey();
				if (!ec.contains(ck))
					iter.remove();
			}
		}
		return in;
	}
	
}
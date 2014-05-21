// OS_STATUS: public
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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatementUtils;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;

/*
 * We started out with A loj B, where A is broadcast distributed and B is not.
 * We execute this as:
 * [1] A ij B redist to a single site on the persistent group
 * [2] On that single site, execute A loj T1
 */
public class BCastToPGOuterJoinStrategy extends JoinStrategy {

	public BCastToPGOuterJoinStrategy(PlannerContext pc, JoinEntry p,
			StrategyTable leftTable,
			StrategyTable rightTable,
			DMLExplainRecord rec) {
		super(pc, p, leftTable, rightTable, rec);
	}

	@Override
	public JoinedPartitionEntry build() throws PEException {
		ExecutionCost combinedCost = BinaryJoinEntry.combineScores(getSchemaContext(), 
				left.getEntry().getScore(), 
				right.getEntry().getScore(), 
				rje.getJoin()); 
		combinedCost.setSingleSite();

		
		ProjectingFeatureStep leftStep = (ProjectingFeatureStep) left.getEntry().getStep(null);
		ProjectingFeatureStep rightStep = (ProjectingFeatureStep) right.getEntry().getStep(null);
		
		SelectStatement lhs = (SelectStatement) leftStep.getPlannedStatement();
		SelectStatement rhs = (SelectStatement) rightStep.getPlannedStatement();

		PEStorageGroup targGroup = null;
		boolean isLeftJoin = getJoin().getJoinType().isLeftOuterJoin();
		if (isLeftJoin) {
			right.getEntry().maybeForceDoublePrecision(rightStep);
			targGroup = left.getGroup();
		} else {
			left.getEntry().maybeForceDoublePrecision(leftStep);
			targGroup = right.getGroup();
		}

		SelectStatement actualJoinStatement = DMLStatementUtils.compose(getSchemaContext(),rhs,lhs);
		UniquedExpressionList uel = new UniquedExpressionList();
		uel.addAll(ExpressionUtils.decomposeAndClause(actualJoinStatement.getWhereClause()));

		// we will always have the original join entry for this strategy -
		uel.addAll(ExpressionUtils.decomposeAndClause((FunctionCall) actualJoinStatement.getMapper().copyForward(getJoin().getJoin().getJoinOn())));
		List<ExpressionNode> ands = uel.getExprs();
				
		// now that the join condition is mapped - we need to get rid of everything from the bcast table in the projection -
		// we're going to pick it up later on when we do the actual join.
		PartitionEntry keepEntry = (isLeftJoin ? right.getEntry() : left.getEntry());
		Set<TableKey> keepTabs = keepEntry.getSpanningTables();
		for(Iterator<ExpressionNode> iter = actualJoinStatement.getProjectionEdge().iterator(); iter.hasNext();) {
			ExpressionNode en = iter.next();
			ExpressionNode target = ExpressionUtils.getTarget(en);
			if (target instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance)target;
				TableKey tk = ci.getTableInstance().getTableKey();
				if (!keepTabs.contains(tk))
					iter.remove();
			}
		}

		actualJoinStatement.setWhereClause(ExpressionUtils.safeBuildAnd(ands));
		actualJoinStatement.normalize(getSchemaContext());

		// so - we have the join - it's on the src group - build a new proj feature step for it
		ProjectingFeatureStep innerJoin =
				DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(
						getPlannerContext(),
						rje.getFeaturePlanner(),
						actualJoinStatement,
						combinedCost,
						leftStep.getSourceGroup(), 
						leftStep.getDatabase(getPlannerContext()),
						(isLeftJoin ? rightStep.getDistributionVector() : leftStep.getDistributionVector()),
						null,
						DMLExplainReason.BCAST_TO_PG_OUTER_JOIN.makeRecord());
		
		// if either side has dependents, they become mine
		innerJoin.getSelfChildren().addAll(left.getEntry().getStep(null).getAllChildren());
		innerJoin.getSelfChildren().addAll(right.getEntry().getStep(null).getAllChildren());
		
		// now remove the inner table from the mapper - we're going to join against it again in the next step
		PartitionEntry puntEntry = (isLeftJoin ? left.getEntry() : right.getEntry());
		for(TableKey tk : puntEntry.getSpanningTables())
			actualJoinStatement.getMapper().remove(tk);

		// now we're going to redist the inner join back onto targ group - we'll use bcast dist
		targGroup = targGroup.anySite(getSchemaContext());

		RedistFeatureStep redisted =
				innerJoin.redist(getPlannerContext(),
						rje.getFeaturePlanner(),
						new TempTableCreateOptions(Model.BROADCAST,targGroup)
							.withRowCount(rje.getScore().getRowCount()),
						null,
						DMLExplainReason.BCAST_TO_PG_OUTER_JOIN.makeRecord());

		if (isLeftJoin) {
			lhs = (SelectStatement) leftStep.getPlannedStatement();
			rhs = redisted.buildNewSelect(getPlannerContext());
			right.getEntry().setStep(redisted);
		} else {
			lhs = redisted.buildNewSelect(getPlannerContext());
			left.getEntry().setStep(redisted);
			rhs = (SelectStatement) rightStep.getPlannedStatement();
		}

		return buildResultEntry(
				lhs,
				leftStep.getDistributionVector(),
				rhs,
				rightStep.getDistributionVector(),
				combinedCost,
				targGroup,
				false);		
	}


}

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatementUtils;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.ListSet;

abstract class JoinStrategy {
	
	protected JoinEntry rje;
	protected StrategyTable left;
	protected StrategyTable right;
	protected DMLExplainRecord explain;
	protected final PlannerContext context;
	
	public JoinStrategy(
			PlannerContext context,
			JoinEntry p,
			StrategyTable leftTable,
			StrategyTable rightTable,
			DMLExplainRecord dmle) {
		rje = p;
		left = leftTable;
		right = rightTable;
		explain = dmle;
		this.context = context;
	}
	
	protected ExecutionCost buildCombinedCost() throws PEException {
		return BinaryJoinEntry.combineScores(getSchemaContext(), 
				left.getEntry().getScore(), 
				right.getEntry().getScore(), 
				rje.getJoin());
	}

	public SchemaContext getSchemaContext() {
		return context.getContext();
	}
	
	public PlannerContext getPlannerContext() {
		return context;
	}
	
	public DGJoin getJoin() {
		return rje.getJoin();
	}
	
	public abstract JoinedPartitionEntry build() throws PEException;

	public static RedistFeatureStep colocateViaRedist(PlannerContext pc,
			DGJoin dgj,
			TableKey origTable,
			PartitionEntry origEntry,
			Model targetModel,
			PEStorageGroup targetGroup,
			boolean allColumns,
			DMLExplainRecord why,
			FeaturePlanner planner) throws PEException {
		List<ExpressionNode> redistOn = dgj.getRedistJoinExpressions(origTable);
		@SuppressWarnings("unchecked")
		List<ExpressionNode> mro = (targetModel == Model.BROADCAST ? Collections.EMPTY_LIST : origEntry.mapDistributedOn(redistOn)); 
		List<Integer> mappedRedistOn = origEntry.mapDistributedOn(mro, origEntry.getTempTableSource());
		TempTableCreateOptions opts =
				new TempTableCreateOptions(targetModel,targetGroup)
				.withRowCount(origEntry.getScore().getRowCount())
				.distributeOn(mappedRedistOn);
		if (!allColumns)
			opts.withInvisibleColumns(origEntry.getInvisibleColumns());

		if (origEntry.getStep(null) instanceof ProjectingFeatureStep) {
			ProjectingFeatureStep pfs = (ProjectingFeatureStep) origEntry.getStep(null);
			RedistFeatureStep rfs = pfs.redist(pc, 
					planner,
					opts,
					null, 
					why);
			origEntry.maybeForceDoublePrecision(pfs);
			return rfs;
		} else {
			throw new PEException("Invalid redist state - expected a projection but found " + origEntry.getStep(null).getClass().getSimpleName());
		}
		
	}
		
	public String toString() {
		return getClass().getSimpleName() + ":{" + left.getEntry() + ", " + right.getEntry() + "}";
	}
	
	protected JoinedPartitionEntry buildResultEntry(
			SelectStatement lhs,
			DistributionVector lVect,
			SelectStatement rhs,
			DistributionVector rVect,
			ExecutionCost finalCost,
			PEStorageGroup targetGroup,
			boolean parallelChildren) throws PEException {

		SelectStatement finalJoinKern = DMLStatementUtils.compose(getSchemaContext(),
				lhs, rhs);
		
		ListSet<DistributionVector> vectors = new ListSet<DistributionVector>();
		vectors.add(lVect);
		vectors.add(rVect);

		ArrayList<PartitionEntry> components = new ArrayList<PartitionEntry>();
		components.add(left.getEntry());
		components.add(right.getEntry());

		DMLExplainRecord boundExplain = explain;
		if (boundExplain != null && finalCost.getRowCount() > -1)
			boundExplain = boundExplain.withRowEstimate(finalCost.getRowCount());

		JoinedPartitionEntry sipe = 
				new JoinedPartitionEntry(getPlannerContext(),
						getSchemaContext(), 
						rje.getBasis(),
						finalJoinKern,
						components,
						targetGroup, 
						vectors,
						finalCost,
						rje.getParentTransform(),
						rje.getFeaturePlanner(),
						rje.isOuterJoin(),
						parallelChildren,
						boundExplain);

		return sipe;
	}

	
}
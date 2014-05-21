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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;

/*
 * This strategy colocates the partitions by redistributing the more constrained partition
 * back onto the persistent group broadcast and executing the join. 
 */
class BCastToPGJoinStrategy extends JoinStrategy {

	private final StrategyTable constrainedSide;
	
	public BCastToPGJoinStrategy(PlannerContext pc, JoinEntry p,
			StrategyTable leftTable,
			StrategyTable rightTable,
			StrategyTable constrainedSide,
			DMLExplainRecord expl) {
		super(pc, p, leftTable, rightTable, expl);
		this.constrainedSide = constrainedSide;
	}

	@Override
	public JoinedPartitionEntry build() throws PEException {
		ExecutionCost combinedCost = buildCombinedCost();
		
		SelectStatement lhs = null;
		SelectStatement rhs = null;
		
		DistributionVector lVect = null;
		DistributionVector rVect = null;

		PEStorageGroup targetGroup = null;
		
		if (constrainedSide == left) {
			// redist the left onto the right
			RedistFeatureStep rfs = colocateViaRedist(getPlannerContext(),
					getJoin(),
					left.getSingleTable(),
					left.getEntry(),
					Model.BROADCAST,
					right.getEntry().getSourceGroup(),
					false,
					null,
					rje.getFeaturePlanner());
			left.getEntry().setStep(rfs);
			lhs = rfs.buildNewSelect(getPlannerContext());
			lVect = rfs.getTargetTempTable().getDistributionVector(getSchemaContext());

			ProjectingFeatureStep pfs = (ProjectingFeatureStep)right.getEntry().getStep(null); 
			rhs = (SelectStatement) pfs.getPlannedStatement();
			rVect = pfs.getDistributionVector();
			
			targetGroup = right.getEntry().getSourceGroup();
		} else {
			RedistFeatureStep rfs = colocateViaRedist(getPlannerContext(),
					getJoin(),
					right.getSingleTable(),
					right.getEntry(),
					Model.BROADCAST,
					left.getEntry().getSourceGroup(),
					false,
					null,
					rje.getFeaturePlanner());	
			right.getEntry().setStep(rfs);
			rhs = rfs.buildNewSelect(getPlannerContext());
			rVect = rfs.getTargetTempTable().getDistributionVector(getSchemaContext());
			
			ProjectingFeatureStep pfs = (ProjectingFeatureStep)left.getEntry().getStep(null); 
			lhs = (SelectStatement) pfs.getPlannedStatement();
			lVect = pfs.getDistributionVector();
			
			targetGroup = left.getEntry().getSourceGroup();
		}

		return buildResultEntry(lhs,
				lVect,
				rhs,
				rVect,
				combinedCost,
				targetGroup,
				false);
				
	}
	
}
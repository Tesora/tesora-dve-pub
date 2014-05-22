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
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;

/*
 * The regular join strategy proceeds by redistributing both partitions onto the temp group
 * distributed on the join columns, then executes the join on the temp group.
 */
class RegularJoinStrategy extends JoinStrategy {
	
	public RegularJoinStrategy(PlannerContext pc, JoinEntry p,
			StrategyTable leftTable,
			StrategyTable rightTable,
			DMLExplainRecord expl) {
		super(pc, p, leftTable, rightTable, expl);
	}
	
	@Override
	public JoinedPartitionEntry build() throws PEException {
		ExecutionCost combinedCost = buildCombinedCost();
		PEStorageGroup tempGroup = getPlannerContext().getTempGroupManager().getGroup(combinedCost.getGroupScore());
		
		RedistFeatureStep lhs = colocateViaRedist(getPlannerContext(),
				getJoin(),
				left.getSingleTable(),
				left.getEntry(),
				Model.STATIC,
				tempGroup,
				false,
				DMLExplainReason.JOIN.makeRecord(),
				rje.getFeaturePlanner());

		left.getEntry().setStep(lhs);
		
		RedistFeatureStep rhs = colocateViaRedist(getPlannerContext(),
				getJoin(),
				right.getSingleTable(),
				right.getEntry(),
				Model.STATIC,
				tempGroup,
				false,
				DMLExplainReason.JOIN.makeRecord(),
				rje.getFeaturePlanner());

		right.getEntry().setStep(rhs);
		
		return buildResultEntry(lhs.buildNewSelect(getPlannerContext()),
				lhs.getTargetTempTable().getDistributionVector(getSchemaContext()),
				rhs.buildNewSelect(getPlannerContext()),
				rhs.getTargetTempTable().getDistributionVector(getSchemaContext()),
				combinedCost,
				tempGroup, 
				true);
	}
	
	
}
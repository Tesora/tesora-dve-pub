// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

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
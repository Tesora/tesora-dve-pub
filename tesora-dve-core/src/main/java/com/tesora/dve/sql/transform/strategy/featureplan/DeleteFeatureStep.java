// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.transform.execution.DeleteExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public class DeleteFeatureStep extends NonQueryFeatureStep {

	public DeleteFeatureStep(FeaturePlanner planner, DeleteStatement ds, TableKey onTable, PEStorageGroup srcGroup, DistributionKey dk) {
		super(planner, ds, onTable, srcGroup, dk);
	}
	
	@Override
	public void scheduleSelf(PlannerContext pc, ExecutionSequence es)
			throws PEException {
		es.append(DeleteExecutionStep.build(
				pc.getContext(),
				getTable().getAbstractTable().getDatabase(pc.getContext()),
				getSourceGroup(),
				getTable(),
				getDistributionKey(),
				getPlannedStatement(),
				getPlannedStatement().getDerivedInfo().doSetTimestampVariable(),
				getExplainRecord()));
	}

}

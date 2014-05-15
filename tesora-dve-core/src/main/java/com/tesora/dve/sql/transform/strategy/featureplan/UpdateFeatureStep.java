// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;


import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.UpdateExecutionStep;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public class UpdateFeatureStep extends NonQueryFeatureStep {
	
	public UpdateFeatureStep(FeaturePlanner planner, DMLStatement us, TableKey onTab, PEStorageGroup group, DistributionKey dk) {
		super(planner, us, onTab, group,dk);
	}
	
	@Override
	public void scheduleSelf(PlannerContext pc, ExecutionSequence es) throws PEException {
		es.append(UpdateExecutionStep.build(
				pc.getContext(),
				getTable().getAbstractTable().getDatabase(pc.getContext()),
				getSourceGroup(),
				getTable().getAbstractTable().asTable(),
				getDistributionKey(),
				getPlannedStatement(),
				getPlannedStatement().getDerivedInfo().doSetTimestampVariable(),
				getExplainRecord()));
	}

}

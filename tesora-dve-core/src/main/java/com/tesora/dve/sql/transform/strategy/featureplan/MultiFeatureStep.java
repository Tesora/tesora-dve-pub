// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public class MultiFeatureStep extends FeatureStep {

	public MultiFeatureStep(FeaturePlanner planner) {
		super(planner, null, null);
	}

	@Override
	protected void scheduleSelf(PlannerContext sc, ExecutionSequence es)
			throws PEException {
	}

	@Override
	public DMLStatement getPlannedStatement() {
		return null;
	}

	@Override
	public Database<?> getDatabase(PlannerContext pc) {
		return getSelfChildren().get(0).getDatabase(pc);
	}
	
}

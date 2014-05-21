package com.tesora.dve.sql.transform.strategy;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

public class AdhocFeaturePlanner extends TransformFactory {

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context)
			throws PEException {
		throw new PEException("Invalid plan call - adhoc planner");
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.ADHOC;
	}

}

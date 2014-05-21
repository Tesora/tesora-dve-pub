package com.tesora.dve.sql.transform.behaviors;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

// sometimes we have need to modify a complete plan, after the feature planners are finished
public interface FeaturePlanTransformerBehavior extends FeaturePlannerBehavior {

	FeatureStep transform(PlannerContext pc, DMLStatement stmt, FeatureStep existingPlan) throws PEException;
	
}

package com.tesora.dve.sql.transform.behaviors.defaults;

import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.behaviors.FeaturePlanTransformerBehavior;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

// eventually this will do the wide result set transform
public final class DefaultPostPlanningFeaturePlanTransformer implements
		FeaturePlanTransformerBehavior {

	public static final FeaturePlanTransformerBehavior INSTANCE = new DefaultPostPlanningFeaturePlanTransformer();
	
	@Override
	public FeatureStep transform(PlannerContext pc, DMLStatement stmt,
			FeatureStep existingPlan) {
		return existingPlan;
	}

}

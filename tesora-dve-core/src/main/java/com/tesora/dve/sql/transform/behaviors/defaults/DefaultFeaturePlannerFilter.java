// OS_STATUS: public
package com.tesora.dve.sql.transform.behaviors.defaults;

import com.tesora.dve.sql.transform.behaviors.FeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public final class DefaultFeaturePlannerFilter implements FeaturePlannerFilter {

	public static final FeaturePlannerFilter INSTANCE = new DefaultFeaturePlannerFilter();
	
	public boolean canApply(PlannerContext pc, FeaturePlannerIdentifier id) {
		return !pc.getApplied().contains(id);
	}
	
}

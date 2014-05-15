// OS_STATUS: public
package com.tesora.dve.sql.transform.behaviors;

import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public interface FeaturePlannerFilter extends FeaturePlannerBehavior {

	public boolean canApply(PlannerContext pc, FeaturePlannerIdentifier id);

}

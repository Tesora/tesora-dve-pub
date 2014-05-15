// OS_STATUS: public
package com.tesora.dve.sql.transform.behaviors;

import java.util.HashSet;
import java.util.Set;

import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public class ComplexFeaturePlannerFilter implements FeaturePlannerFilter {

	private final Set<FeaturePlannerIdentifier> not;
	private final Set<FeaturePlannerIdentifier> forceInclude;
	
	public ComplexFeaturePlannerFilter(Set<FeaturePlannerIdentifier> not, Set<FeaturePlannerIdentifier> forceInclude) {
		super();
		this.not = not;
		this.forceInclude = forceInclude;
	}
	
	public boolean canApply(PlannerContext pc, FeaturePlannerIdentifier id) {
		HashSet<FeaturePlannerIdentifier> applied = new HashSet<FeaturePlannerIdentifier>();
		applied.addAll(not);
		for(FeaturePlannerIdentifier ti : pc.getApplied()) {
			if (!forceInclude.contains(ti))
				applied.add(ti);
		}
		return !applied.contains(id);
	}
	
}

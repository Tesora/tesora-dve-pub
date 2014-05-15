// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class PlannerContext {

	private final FixedPlannerContext fixed;
	private boolean requiresCosting = false;
	private boolean usesAggSite = false;
	private final ListSet<FeaturePlannerIdentifier> applied;
	
	public PlannerContext(SchemaContext sc) {
		this.fixed = new FixedPlannerContext(sc);
		this.applied = new ListSet<FeaturePlannerIdentifier>();
	}

	public PlannerContext(PlannerContext other) {
		this.fixed = other.fixed;
		this.requiresCosting = other.requiresCosting;
		this.usesAggSite = other.usesAggSite;
		this.applied = new ListSet<FeaturePlannerIdentifier>(other.applied);
	}

	public SchemaContext getContext() {
		return fixed.getContext();
	}
	
	public PlannerContext withCosting() {
		PlannerContext out = new PlannerContext(this);
		out.requiresCosting = true;
		return out;
	}
		
	public PlannerContext withAggSite() {
		PlannerContext out = new PlannerContext(this);
		out.usesAggSite = true;
		return out;
	}
	
	public PlannerContext withTransform(FeaturePlannerIdentifier id) {
		PlannerContext out = new PlannerContext(this);
		out.applied.add(id);
		return out;
	}
	
	public boolean isCosting() {
		return requiresCosting;
	}
	
	public boolean isAggSite() {
		return usesAggSite;
	}

	public ListSet<FeaturePlannerIdentifier> getApplied() {
		return applied;
	}

	public TempGroupManager getTempGroupManager() {
		return fixed.getTempGroupManager();
	}
	
	public TempTableSanity getTempTableSanity() {
		return fixed.getTempTableSanity();
	}
}

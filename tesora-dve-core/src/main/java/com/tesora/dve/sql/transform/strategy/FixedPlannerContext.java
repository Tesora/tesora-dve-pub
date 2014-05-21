// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultBehaviorConfiguration;

public class FixedPlannerContext {

	private final SchemaContext sc;
	private final TempGroupManager tempGroupManager;
	private final TempTableSanity sanity;
	private final BehaviorConfiguration behaviors; 
	
	public FixedPlannerContext(SchemaContext sc, BehaviorConfiguration config) {
		this.sc = sc;
		this.tempGroupManager = new TempGroupManager();
		this.sanity = new TempTableSanity();
		if (config == null)
			this.behaviors = new DefaultBehaviorConfiguration();
		else
			this.behaviors = config;
	}
	
	public SchemaContext getContext() {
		return sc;
	}
	
	public TempGroupManager getTempGroupManager() {
		return tempGroupManager;
	}
	
	public TempTableSanity getTempTableSanity() {
		return sanity;
	}
	
	public BehaviorConfiguration getBehaviorConfiguration() {
		return behaviors;
	}
}

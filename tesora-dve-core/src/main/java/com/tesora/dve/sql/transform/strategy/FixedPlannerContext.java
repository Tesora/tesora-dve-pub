// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import com.tesora.dve.sql.schema.SchemaContext;

public class FixedPlannerContext {

	private final SchemaContext sc;
	private final TempGroupManager tempGroupManager;
	private final TempTableSanity sanity;

	public FixedPlannerContext(SchemaContext sc) {
		this.sc = sc;
		this.tempGroupManager = new TempGroupManager();
		this.sanity = new TempTableSanity();
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
}

// OS_STATUS: publuic
package com.tesora.dve.sql.transform.strategy;

public class StrategyHint {

	private boolean requiresCosting = false;
	private boolean usesAggSite = false;
	
	public StrategyHint() {
		
	}
	
	public StrategyHint(StrategyHint other) {
		this.requiresCosting = other.requiresCosting;
		this.usesAggSite = other.usesAggSite;
	}
	
	public StrategyHint withCosting() {
		StrategyHint nsh = new StrategyHint(this);
		nsh.requiresCosting = true;
		return nsh;
	}
	
	public StrategyHint withAggSite() {
		StrategyHint sh = new StrategyHint(this);
		sh.usesAggSite = true;
		return sh;
	}
	
	public boolean isCosting() {
		return requiresCosting;
	}
	
	public boolean isAggSite() {
		return usesAggSite;
	}
}

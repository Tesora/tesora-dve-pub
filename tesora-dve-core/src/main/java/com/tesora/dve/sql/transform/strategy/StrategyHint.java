package com.tesora.dve.sql.transform.strategy;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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

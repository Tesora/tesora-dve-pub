package com.tesora.dve.sql.util;

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

import java.util.List;

public class ComparisonOptions {

	public static final ComparisonOptions DEFAULT = new ComparisonOptions();
	
	private boolean ignoreOrder = false;
	private boolean ignoreMD = false;
	private UnaryFunction<List<ColumnChecker>,List<ColumnChecker>> mutator = null;
	
	private ComparisonOptions() {
		
	}
	
	private ComparisonOptions(ComparisonOptions o) {
		this.ignoreOrder = o.ignoreOrder;
		this.ignoreMD = o.ignoreMD;
		this.mutator = o.mutator;
	}
	
	public boolean isIgnoreOrder() {
		return ignoreOrder;
	}
	
	public boolean isIgnoreMD() {
		return ignoreMD;
		//return false;
	}
	
	public UnaryFunction<List<ColumnChecker>,List<ColumnChecker>> getCheckerMutator() {
		return mutator;
	}
	
	public ComparisonOptions withIgnoreOrder() {
		ComparisonOptions out = new ComparisonOptions(this);
		out.ignoreOrder = true;
		return out;
	}
	
	public ComparisonOptions withIgnoreMD() {
		ComparisonOptions out = new ComparisonOptions(this);
		out.ignoreMD = true;
		return out;
	}
	
	public ComparisonOptions withCheckerMutator(UnaryFunction<List<ColumnChecker>,List<ColumnChecker>> f) {
		ComparisonOptions out = new ComparisonOptions(this);
		out.mutator = f;
		return out;
	}
	
}

package com.tesora.dve.sql.util;

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

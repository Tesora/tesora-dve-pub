// OS_STATUS: public
package com.tesora.dve.variable;

public enum VariableScopeKind implements java.io.Serializable {

	DVE,
	SESSION,
	USER,
	GROUP_PROVIDER(true);
	
	private final boolean named;
	
	private VariableScopeKind() {
		this(false);
	}
	
	private VariableScopeKind(boolean n) {
		named = n;
	}
	
	public boolean hasName() {
		return named;
	}
	
	public static VariableScopeKind lookup(String in) {
		return VariableScopeKind.valueOf(in.trim().toUpperCase());
	}
	
}

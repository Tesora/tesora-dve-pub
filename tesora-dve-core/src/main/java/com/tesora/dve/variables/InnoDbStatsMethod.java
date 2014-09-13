package com.tesora.dve.variables;

public enum InnoDbStatsMethod {
	
	NULLS_EQUAL("nulls_equal"), NULLS_UNEQUAL("nulls_unequal"), NULLS_IGNORED("nulls_ignored");
	
	private final String name;

	private InnoDbStatsMethod(final String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return name;
	}

}

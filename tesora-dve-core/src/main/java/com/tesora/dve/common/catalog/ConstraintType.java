// OS_STATUS: public
package com.tesora.dve.common.catalog;

public enum ConstraintType {
	
	FOREIGN("FOREIGN"),
	PRIMARY("PRIMARY"),
	UNIQUE("UNIQUE");

	private final String sql;
	private ConstraintType(String s) {
		sql = s;
	}
	
	public String getSQL() {
		return sql;
	}
	
}

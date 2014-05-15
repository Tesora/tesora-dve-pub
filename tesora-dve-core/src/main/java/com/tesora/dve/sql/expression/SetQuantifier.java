// OS_STATUS: public
package com.tesora.dve.sql.expression;

public enum SetQuantifier {

	DISTINCT,
	ALL;
	
	public static SetQuantifier fromSQL(String in) {
		String l = in.trim().toUpperCase();
		for(SetQuantifier sq : SetQuantifier.values()) {
			if (sq.name().equals(l))
				return sq;
		}
		return null;
	}
	
	public String getSQL() {
		return name();
	}
	
}

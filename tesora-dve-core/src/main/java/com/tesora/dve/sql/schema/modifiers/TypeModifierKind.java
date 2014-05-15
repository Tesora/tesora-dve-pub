// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

public enum TypeModifierKind {

	UNSIGNED("UNSIGNED"),
	ZEROFILL("ZEROFILL"),
	BINARY("BINARY"),
	CHARSET("CHARSET"),
	COLLATE("COLLATE"),
	COMPARISON("COMPARATOR"),
	SIGNED("SIGNED");

	private final String sql;
	private TypeModifierKind(String s) {
		sql = s;
	}
	
	public String getSQL() {
		return sql;
	}
}

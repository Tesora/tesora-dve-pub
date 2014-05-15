// OS_STATUS: public
package com.tesora.dve.infomessage;

public enum Level {

	// order here is important, we select based on this order
	NOTE("Note","NOTES"),
	WARNING("Warning","WARNINGS"),
	ERROR("Error","ERRORS");
	
	private final String resultSetName;
	private final String sqlName;
	
	private Level(String rsn, String sqln) {
		resultSetName = rsn;
		sqlName = sqln;
	}
	
	public String getResultSetName() {
		return resultSetName;
	}
	
	public String getSQLName() {
		return sqlName;
	}
	
	public static Level findLevel(String sql) {
		if (sql == null) return null;
		for(Level l : Level.values()) {
			if (l == NOTE) continue;
			if (l.getSQLName().equalsIgnoreCase(sql))
				return l;
		}
		return null;
	}
}

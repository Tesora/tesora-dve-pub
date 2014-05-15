// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.Locale;

public enum ForeignKeyAction {

	RESTRICT("RESTRICT","RESTRICT"),
	CASCADE("CASCADE","CASCADE"),
	SET_NULL("SET NULL","SET NULL"),
	NO_ACTION("NO ACTION","NO ACTION");
	
	private String sql;
	private String persistent;
	
	private ForeignKeyAction(String sql, String pers) {
		this.sql = sql;
		this.persistent = pers;
	}
	
	public String getSQL() {
		return this.sql;
	}
	
	public String getPersistent() {
		return this.persistent;
	}
	
	public static ForeignKeyAction fromPersistent(String in) {
		if (in == null) return NO_ACTION;
		String uc = in.toUpperCase(Locale.ENGLISH);
		for(ForeignKeyAction fka : ForeignKeyAction.values()) {
			if (fka.getPersistent().equals(uc))
				return fka;
		}
		return null;
	}
	
	public static ForeignKeyAction fromSQL(String in) {
		if (in == null) return NO_ACTION;
		String uc = in.toUpperCase(Locale.ENGLISH);
		for(ForeignKeyAction fka : ForeignKeyAction.values()) {
			if (fka.getSQL().equals(uc))
				return fka;
		}
		return null;
	}
};

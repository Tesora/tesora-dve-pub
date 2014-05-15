// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

import java.util.Locale;

public enum InsertModifier {
	DELAYED("DELAYED"),
	HIGH_PRIORITY("HIGH_PRIORITY"),
	LOW_PRIORITY("LOW_PRIORITY");
	
	private String sql;
	
	private InsertModifier(String sql) {
		this.sql = sql;
	}
	
	public String getSQL() {
		return this.sql;
	}
	
	public static InsertModifier fromSQL(String in) {
		if (in == null) return null;
		String uc = in.toUpperCase(Locale.ENGLISH);
		for(InsertModifier im : InsertModifier.values()) {
			if (im.getSQL().equals(uc))
				return im;
		}
		return null;
	}
}

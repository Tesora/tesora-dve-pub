// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.Locale;

public enum LoadDataInfileModifier {
	CONCURRENT("CONCURRENT"),
	LOW_PRIORITY("LOW_PRIORITY");
	
	private String sql;
	
	private LoadDataInfileModifier(String sql) {
		this.sql = sql;
	}
	
	public String getSQL() {
		return this.sql;
	}
	
	public static LoadDataInfileModifier fromSQL(String in) {
		if (in == null) return null;
		String uc = in.toUpperCase(Locale.ENGLISH);
		for(LoadDataInfileModifier im : LoadDataInfileModifier.values()) {
			if (im.getSQL().equals(uc))
				return im;
		}
		return null;
	}
}

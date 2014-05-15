// OS_STATUS: public
package com.tesora.dve.common.catalog;

public enum FKMode {

	STRICT("strict"),
	IGNORE("ignore"),
	EMULATE("emulate");
	
	private final String persmode;
	
	private FKMode(String pers) {
		persmode = pers;
	}
	
	public String getPersistentValue() {
		return persmode;
	}
	
	public static FKMode toMode(String in) {
		if (in == null) return null;
		for(FKMode fkm : FKMode.values()) {
			if (fkm.getPersistentValue().equalsIgnoreCase(in))
				return fkm;
		}
		return null;
	}
	
}

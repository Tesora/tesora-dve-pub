// OS_STATUS: public
package com.tesora.dve.common.catalog;

public enum ViewMode {

	ACTUAL("passthrough"),
	EMULATE("emulate");
	
	private final String persmode;
	
	private ViewMode(String pers) {
		persmode = pers;
	}
	
	public String getPersistentValue() {
		return persmode;
	}
	
	public static ViewMode toMode(String in) {
		if (in == null) return null;
		for(ViewMode fkm : ViewMode.values()) {
			if (fkm.getPersistentValue().equalsIgnoreCase(in))
				return fkm;
		}
		return null;
	}

	
}

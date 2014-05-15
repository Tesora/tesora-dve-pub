// OS_STATUS: public
package com.tesora.dve.common.catalog;

import java.util.Locale;

public enum MultitenantMode {

	OFF("off"),
	ADAPTIVE("adaptive");
	
	private final String persmode;
	
	private MultitenantMode(String v) {
		persmode = v;
	}
	
	public boolean isMT() {
		return this != OFF;
	}
	
	public String getPersistentValue() {
		return persmode;
	}
	
	public String describe() {
		if (this == OFF)
			return "nonmultitenant";
		else 
			return getPersistentValue() + " multitenant";
	}
	
	public static MultitenantMode toMode(String in) {
		if (in == null) return null;
		String lc = in.toLowerCase(Locale.ENGLISH);
		for(MultitenantMode mm : MultitenantMode.values()) {
			if (mm.persmode.equals(lc))
				return mm;
		}
		return null;
	}
	
}

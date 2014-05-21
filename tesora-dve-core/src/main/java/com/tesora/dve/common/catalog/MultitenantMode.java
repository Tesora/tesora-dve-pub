// OS_STATUS: public
package com.tesora.dve.common.catalog;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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

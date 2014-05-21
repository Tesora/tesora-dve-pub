// OS_STATUS: public
package com.tesora.dve.siteprovider;

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

import org.apache.commons.lang.StringUtils;

public enum DynamicSiteStatus {
	ONLINE("ONLINE"), OFFLINE("OFFLINE"), FAILED("FAILED"), DELETED("DELETED");

	private String value;

	private DynamicSiteStatus(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return this.value;
	}

	public static DynamicSiteStatus fromString(String s) {
		if (s == null || "".equals(s)) 
			throw new IllegalArgumentException("Value cannot be null or empty!");
		
		for (DynamicSiteStatus sis : values()) {
			if (StringUtils.equalsIgnoreCase(sis.name(), s)) {
				return sis;
			}
		}
		throw new IllegalArgumentException("'" + s + "' is not a valid staus value");
	}

	public boolean isValidUserOption() {
		return StringUtils.equalsIgnoreCase(ONLINE.name(), value) || StringUtils.equalsIgnoreCase(OFFLINE.name(), value);
	}
	
	public static boolean isValidUserOption(String option) {
		for (DynamicSiteStatus s : values()) {
			if ((s == ONLINE || s == OFFLINE) && StringUtils.equalsIgnoreCase(s.name(), option)) {
				return true;
			}
		}
		return false;
	}
}
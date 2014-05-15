// OS_STATUS: public
package com.tesora.dve.siteprovider;

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
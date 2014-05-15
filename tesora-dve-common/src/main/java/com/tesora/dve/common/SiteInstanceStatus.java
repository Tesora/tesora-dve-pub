// OS_STATUS: public
package com.tesora.dve.common;

import org.apache.commons.lang.StringUtils;

public enum SiteInstanceStatus {
	ONLINE("ONLINE"), OFFLINE("OFFLINE"), FAILED("FAILED");

	private String value;

	private SiteInstanceStatus(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return this.value;
	}

	public static SiteInstanceStatus fromString(String s) {
		for (SiteInstanceStatus sis : values()) {
			if (StringUtils.equalsIgnoreCase(sis.name(), s)) {
				return sis;
			}
		}
		return null;
	}

	public static boolean isValidUserOption(String option) {
		for (SiteInstanceStatus s : values()) {
			if ((s != FAILED) && StringUtils.equalsIgnoreCase(s.name(), option)) {
				return true;
			}
		}
		return false;
	}
}
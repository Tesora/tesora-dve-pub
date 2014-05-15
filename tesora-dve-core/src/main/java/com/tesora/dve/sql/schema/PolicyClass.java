// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.common.PEConstants;

public enum PolicyClass {

	AGGREGATE(PEConstants.AGGREGATE),
	SMALL(PEConstants.SMALL),
	MEDIUM(PEConstants.MEDIUM),
	LARGE(PEConstants.LARGE);
	
	private final String persistent;
	private final String sql;
	
	private PolicyClass(String p) {
		this.persistent = p;
		this.sql = p.toUpperCase();
	}
	
	public String getSQL() {
		return this.sql;
	}
	
	public String getPersistent() {
		return persistent;
	}
	
	public static PolicyClass findSQL(String v) {
		for(PolicyClass pc : PolicyClass.values()) {
			if (pc.getSQL().equalsIgnoreCase(v))
				return pc;
		}
		return null;
	}
	
	public static PolicyClass findPersistent(String v) {
		for(PolicyClass pc : PolicyClass.values()) {
			if (pc.getPersistent().equalsIgnoreCase(v))
				return pc;
		}
		return null;
	}
}

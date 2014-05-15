// OS_STATUS: public
package com.tesora.dve.worker;

public class DevXid {
	public static final int FORMAT_ID = 124;

	String xidString;

	public DevXid() {
	}

	public DevXid(String globalId, String workerId) {
		xidString = "'" + globalId + "','" + workerId + "'," + FORMAT_ID;
	}

	@Override
	public String toString() {
		return "{Xid " + xidString + "}";
	}

	public String getMysqlXid() {
		return xidString;
	}
}
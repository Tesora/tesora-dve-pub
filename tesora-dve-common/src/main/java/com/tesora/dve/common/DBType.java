// OS_STATUS: public
package com.tesora.dve.common;

public enum DBType {

	MYSQL(PEConstants.MYSQL_DRIVER_CLASS),
	MARIADB(PEConstants.MARIADB_DRIVER_CLASS),
	
	UNKNOWN("");

	private final String driverClass;

	private DBType(String dc) {
		this.driverClass = dc;
	}

	public static DBType fromDriverClass(String dc) {
		for (DBType dbt : values()) {
			if (dbt.driverClass.equals(dc)) {
				return dbt;
			}
		}
		return UNKNOWN;
	}
}

// OS_STATUS: public
package com.tesora.dve.sql.util;



public abstract class DatabaseDDL extends TestDDL {

	protected String dbn;
	protected String dbtag;

	public DatabaseDDL(String dbname, String dbtag) {
		this.dbn = dbname;
		this.dbtag = dbtag;
	}

	public abstract boolean isNative();
	
	public String getDatabaseName() {
		return dbn;
	}

	public abstract String getDropStatement();
	
	public abstract DatabaseDDL copy();
	
	public abstract String getCreateDatabaseStatement();
	
}

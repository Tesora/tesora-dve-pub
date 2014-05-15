// OS_STATUS: public
package com.tesora.dve.sql.util;

import java.util.ArrayList;
import java.util.List;

public class NativeDDL extends ProjectDDL {

	public NativeDDL() {
		super();
	}
	
	public NativeDDL(String dbn) {
		super();
		withDatabase(new NativeDatabaseDDL(dbn));
	}

	@Override
	public List<String> getCreateStatements() throws Exception {
		ArrayList<String> buf = new ArrayList<String>();
		List<DatabaseDDL> mydbs = getDatabases();
		for(DatabaseDDL ddl : mydbs)
			buf.addAll(ddl.getCreateStatements());
		// convention is use first
		buf.add("use " + mydbs.get(0).getDatabaseName());
		return buf;
	}
	
	@Override
	public boolean isNative() {
		return true;
	}

	@Override
	public List<String> getDestroyStatements() throws Exception {
		ArrayList<String> buf = new ArrayList<String>();
		for(DatabaseDDL ddl : getDatabases())
			buf.add("drop database if exists " + ddl.getDatabaseName());
		return buf;
	}
	
	@Override
	public List<String> getSetupDrops() {
		ArrayList<String> buf = new ArrayList<String>();
		for(DatabaseDDL ddl : getDatabases()) {
			buf.add("DROP DATABASE IF EXISTS " + ddl.getDatabaseName());
		}
		return buf;
	}

	@Override
	public StorageGroupDDL getPersistentGroup() {
		throw new IllegalStateException("native ddl doesn't have persistent groups");
	}

	@Override
	public ProjectDDL buildTenantDDL(String tenantName) {
		throw new IllegalStateException("native ddl doesn't have tenant ddl");
	}

	
	
}
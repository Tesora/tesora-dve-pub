// OS_STATUS: public
package com.tesora.dve.sql.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NativeDatabaseDDL extends DatabaseDDL {

	public NativeDatabaseDDL(String dbname, String dbtag) {
		super(dbname, dbtag);
	}

	public NativeDatabaseDDL(String dbname) {
		this(dbname,"database");
	}

	@Override
	public boolean isNative() {
		return true;
	}

	@Override
	public DatabaseDDL copy() {
		return new NativeDatabaseDDL(dbn,dbtag);
	}

	@Override
	public List<String> getCreateStatements() throws Exception {
		ArrayList<String> buf = new ArrayList<String>();
		if (isCreated())
			buf.add("drop database if exists " + getDatabaseName());
		buf.add("create database if not exists " + getDatabaseName());
		setCreated();
		buf.add("use " + getDatabaseName());
		return buf;
	}

	@Override
	public List<String> getDestroyStatements() throws Exception {
		return Collections.singletonList("drop database if exists " + getDatabaseName());
	}
	
	@Override
	public List<String> getSetupDrops() {
		ArrayList<String> buf = new ArrayList<String>();
		buf.add("DROP DATABASE IF EXISTS " + dbn);
		return buf;
	}

	@Override
	public String getCreateDatabaseStatement() {
		return "create database " + dbn;
	}

	@Override
	public String getDropStatement() {
		return "drop database if exists " + dbn;
	}
	
}

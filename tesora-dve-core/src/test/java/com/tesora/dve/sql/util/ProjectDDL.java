// OS_STATUS: public
package com.tesora.dve.sql.util;


import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public abstract class ProjectDDL extends TestDDL {

	boolean throwIfSingleDB = true;
	
	protected List<DatabaseDDL> dbs = new ArrayList<DatabaseDDL>();
	
	public ProjectDDL() {
	}
	
	public ProjectDDL(ProjectDDL other) {
		this.created = other.created;
		this.dbs = new ArrayList<DatabaseDDL>();
		for(DatabaseDDL ddl : other.dbs)
			this.dbs.add(ddl.copy());
	}
	
		
	public boolean isNative() {
		return false;
	}

	public ProjectDDL withDatabase(DatabaseDDL ddl) {
		this.dbs.add(ddl);
		return this;
	}
	
	public abstract StorageGroupDDL getPersistentGroup();
	
	public List<String> getCreateDatabaseStatements() {
		return Functional.apply(dbs, new UnaryFunction<String,DatabaseDDL>() {

			@Override
			public String evaluate(DatabaseDDL object) {
				return object.getCreateDatabaseStatement();
			}
			
		});
	}

	public String getCreateDatabaseStatement() {
		return getSingleDB().getCreateDatabaseStatement();
	}

	public String getDatabaseName() {
		return getSingleDB().getDatabaseName();
	}
	
	protected DatabaseDDL getSingleDB() {
		if (isThrowIfSingleDB() && dbs.size() != 1)
			throw new IllegalStateException("More than one database found");
		return dbs.get(0);
	}
	
	
	
	public abstract ProjectDDL buildTenantDDL(String tenantName);

	public List<DatabaseDDL> getDatabases() {
		return dbs;
	}
	
	// clear is a deep operation, but set is not
	@Override
	public void clearCreated() {
		for(DatabaseDDL ddl : dbs)
			ddl.clearCreated();
		super.clearCreated();
	}

	public boolean isThrowIfSingleDB() {
		return throwIfSingleDB;
	}

	public void setThrowIfSingleDB(boolean throwIfSingleDB) {
		this.throwIfSingleDB = throwIfSingleDB;
	}
}
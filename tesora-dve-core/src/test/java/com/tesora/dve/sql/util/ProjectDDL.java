package com.tesora.dve.sql.util;

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
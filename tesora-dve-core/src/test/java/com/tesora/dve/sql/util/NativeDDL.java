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

public class NativeDDL extends ProjectDDL {

	public NativeDDL() {
		super();
	}
	
	public NativeDDL(String dbn) {
		super();
		withDatabase(new NativeDatabaseDDL(dbn));
	}
	
	public NativeDDL(NativeDatabaseDDL db) {
		super();
		withDatabase(db);
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
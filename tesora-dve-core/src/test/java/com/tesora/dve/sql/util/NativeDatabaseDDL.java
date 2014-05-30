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
import java.util.Collections;
import java.util.List;

public class NativeDatabaseDDL extends DatabaseDDL {

	public NativeDatabaseDDL(String dbname, String dbtag) {
		this(dbname,dbtag,null,null);
	}
	
	public NativeDatabaseDDL(String dbname, String dbtag, String charset, String collation) {
		super(dbname, dbtag, charset, collation);
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
		buf.add("create database if not exists " + getDatabaseName() +
				(charset == null ? "" : " default character set " + charset) + 
				(collation == null ? "" : " default collate " + collation));
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

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



public abstract class DatabaseDDL extends TestDDL {

	protected String dbn;
	protected String dbtag;
	protected String charset = null;
	protected String collation = null;
	
	public DatabaseDDL(String dbname, String dbtag, String charset, String collation) {
		this.dbn = dbname;
		this.dbtag = dbtag;
		this.charset = charset;
		this.collation = collation;
	}

	public abstract boolean isNative();
	
	public String getDatabaseName() {
		return dbn;
	}

	public abstract String getDropStatement();
	
	public abstract DatabaseDDL copy();
	
	public abstract String getCreateDatabaseStatement();
	
}

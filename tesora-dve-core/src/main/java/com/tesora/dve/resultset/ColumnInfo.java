package com.tesora.dve.resultset;

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

import java.io.Serializable;
import java.util.EnumSet;

public class ColumnInfo implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private String columnName;
	private String columnAlias;
	private EnumSet<ColumnAttribute> attrs;
	private String dbName;
	private String tableName;
	
	public ColumnInfo(String name, String alias) {
		columnName = name;
		columnAlias = alias;
		attrs = EnumSet.noneOf(ColumnAttribute.class);
	}
	
	public ColumnInfo setAttribute(ColumnAttribute ca) {
		attrs.add(ca);
		return this;
	}

	public ColumnInfo setDatabaseAndTable(String db, String tbl) {
		dbName = db;
		tableName = tbl;
		return this;
	}
	
	public String getName() {
		return columnName;
	}
	
	public String getAlias() {
		if (columnAlias == null)
			return columnName;
		return columnAlias;
	}		
	
	public String getDatabaseName() {
		return dbName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public boolean isSet(ColumnAttribute ca) {
		return attrs.contains(ca);
	}	
	
	protected void unionize() {
		// the union rules seem to be - if we have an alias, use that as the column name
		// clear the table name, database name
		String aliasWas = columnAlias;
		tableName = null;
		dbName = null;
		if (aliasWas != null) {
			columnName = aliasWas;
			columnAlias = null;
		}
	}
	
	@Override
	public String toString() {
		return "ColumnInfo{dbName=" + (dbName == null ? "null" : dbName) + "; tableName=" + (tableName == null ? "null" : tableName)
				+ "; columnName=" + (columnName == null ? "null" : columnName) + "; alias=" + (columnAlias == null ? "null" : columnAlias);
	}
}
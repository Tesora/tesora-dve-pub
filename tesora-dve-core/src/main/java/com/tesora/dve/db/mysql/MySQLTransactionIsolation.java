package com.tesora.dve.db.mysql;

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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public enum MySQLTransactionIsolation {

	READ_COMMITTED("READ-COMMITTED",Connection.TRANSACTION_READ_COMMITTED),
	READ_UNCOMMITTED("READ-UNCOMMITTED",Connection.TRANSACTION_READ_UNCOMMITTED),
	REPEATABLE_READ("REPEATABLE-READ",Connection.TRANSACTION_REPEATABLE_READ),
	SERIALIZABLE("SERIALIZABLE",Connection.TRANSACTION_SERIALIZABLE);
	
	private final String externalName;
	private final int jdbcValue;
	
	private MySQLTransactionIsolation(String externalName, int jdbcConstant) {
		this.externalName = externalName;
		this.jdbcValue = jdbcConstant;
	}
	
	public String getExternalName() {
		return externalName;
	}
	
	public int getJdbcConstant() {
		return jdbcValue;
	}
	
	public static MySQLTransactionIsolation find(String in) {
		String uc = in.toUpperCase(Locale.ENGLISH);
		for(MySQLTransactionIsolation m : values()) {
			if (m.getExternalName().equals(uc))
				return m;
		}
		return null;
	}
	
	public static MySQLTransactionIsolation find(int level) {
		for(MySQLTransactionIsolation m : values()) {
			if (m.getJdbcConstant() == level)
				return m;
		}
		return null;
	}
	
	// for error handing
	public static List<String> getExternalValuesAsList() {
		ArrayList<String> out = new ArrayList<String>(values().length);
		for(MySQLTransactionIsolation m : values())
			out.add(m.getExternalName());
		return out;
	}
	
}

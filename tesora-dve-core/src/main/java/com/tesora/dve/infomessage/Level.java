// OS_STATUS: public
package com.tesora.dve.infomessage;

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

public enum Level {

	// order here is important, we select based on this order
	NOTE("Note","NOTES"),
	WARNING("Warning","WARNINGS"),
	ERROR("Error","ERRORS");
	
	private final String resultSetName;
	private final String sqlName;
	
	private Level(String rsn, String sqln) {
		resultSetName = rsn;
		sqlName = sqln;
	}
	
	public String getResultSetName() {
		return resultSetName;
	}
	
	public String getSQLName() {
		return sqlName;
	}
	
	public static Level findLevel(String sql) {
		if (sql == null) return null;
		for(Level l : Level.values()) {
			if (l == NOTE) continue;
			if (l.getSQLName().equalsIgnoreCase(sql))
				return l;
		}
		return null;
	}
}

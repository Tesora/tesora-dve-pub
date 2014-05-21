package com.tesora.dve.sql.statement.session;

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

import java.util.Locale;

public enum LockType {
	READ("READ",null),
	READ_LOCAL("READ","LOCAL"),
	WRITE(null, "WRITE"),
	LOW_PRIORITY_WRITE("LOW_PRIORITY","WRITE");
	private final String l;
	private final String r;
	private LockType(String first, String second) {
		l = first;
		r = second;
	}
	
	public String getSQL() {
		return (l == null ? "" : l) + " " + (r == null ? "" : r);
	}
	
	private static boolean nullEquals(String l, String r) {
		return (l == null && r == null) || (l != null && r != null && l.equals(r));
	}
	
	public static LockType fromSQL(String il, String ir) {
		String l = (il == null ? null : il.toUpperCase(Locale.ENGLISH).trim());
		String r = (ir == null ? null : ir.toUpperCase(Locale.ENGLISH).trim());
		for(LockType lt : LockType.values()) {
			if (nullEquals(lt.l,l) && nullEquals(lt.r,r))
				return lt;
		}
		return null;
	}
}
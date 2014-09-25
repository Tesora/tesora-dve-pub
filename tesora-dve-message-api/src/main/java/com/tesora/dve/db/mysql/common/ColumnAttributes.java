package com.tesora.dve.db.mysql.common;

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

public class ColumnAttributes {

	public static final short NOT_NULLABLE = 1;
	public static final short AUTO_INCREMENT = 2;
	public static final short ONUPDATE = 4;
	public static final short KEY_PART = 8;
	public static final short PRIMARY_KEY_PART = 16;
	public static final short UNIQUE_KEY_PART = 32;
	public static final short HAS_DEFAULT_VALUE = 64;
	// i.e. a literal, not an expression
	// just putting this in for now, I think we can get rid of this
	public static final short CONSTANT_DEFAULT_VALUE = 128; 
	// type - information, does the associated type have a size
	public static final short SIZED_TYPE = 256;
	// and does it have a precision/scale
	public static final short PS_TYPE = 512;
	// whether it is unsigned
	public static final short UNSIGNED = 1024;
	// whether it is zerofill
	public static final short ZEROFILL = 2048;
	// binary text, will probably get rid of this later
	public static final short BINARY = 4096;
	
	// persistent representation uses an integer
	public static int set(int current, short flag) {
		current |= flag;
		return current;
	}

	public static int set(int current, short flag, Boolean newValue) {
		if (newValue == null || Boolean.FALSE.equals(newValue)) {
			return clear(current,flag);
		} else {
			return set(current,flag);
		}
	}
	
	public static int clear(int current, short flag) {
		current &= ~flag;
		return current;
	}
	
	public static boolean isSet(int current, short flag) {
		return (current & flag) != 0;
	}

	// transient uses a short.
	public static short set(short current, short flag) {
		current |= flag;
		return current;
	}
	
	public static short clear(short current, short flag) {
		current &= ~flag;
		return current;
	}
	
	public static boolean isSet(short current, short flag) {
		return (current & flag) != 0;
	}
	
	public static String buildSQLTest(String current, short flag, String yes, String no) {
		return String.format("case %s & %d when %d then %s else %s end",
				current,flag,flag,yes,no);
	}
	
}

// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

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
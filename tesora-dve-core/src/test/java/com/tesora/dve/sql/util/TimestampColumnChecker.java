// OS_STATUS: public
package com.tesora.dve.sql.util;

import java.sql.Timestamp;

public class TimestampColumnChecker extends ColumnChecker {

	@Override
	protected String equalObjects(Object expected, Object actual) {
		String results = super.equalObjects(expected, actual);
		if (results == null) return null;
		Timestamp lt = (Timestamp) expected;
		Timestamp rt = (Timestamp) actual;
		return results + " (time " + lt.getTime() + "/" + rt.getTime() + ")";
	}
}

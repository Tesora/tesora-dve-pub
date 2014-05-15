// OS_STATUS: public
package com.tesora.dve.sql.util;

import static org.junit.Assert.fail;

import org.apache.commons.lang.ObjectUtils;

public class ColumnChecker {

	public ColumnChecker() {
	}

	public String asString(Object in) {
		if (in == null)
			return "null";
		return in.toString();
	}

	public String debugString(Object in) throws Throwable {
		return asString(in);
	}

	protected String equalObjects(Object expected, Object actual) {
		if (ObjectUtils.equals(expected, actual))
			return null;
		String typeExpected = "";
		String typeActual = "";
		if (expected.getClass() != actual.getClass()) {
			typeExpected = " [" + expected.getClass().toString() + "]";
			typeActual = " [" + actual.getClass().toString() + "]";
		}
		return "Expected: '" + asString(expected) + "'" + typeExpected + "; Actual: '" + asString(actual) + "'" + typeActual;
	}

	public String isEqual(String cntxt, Object expected, Object actual, boolean justCheck) {
		if (expected == null && actual == null)
			return null;
		String diffs = null;
		if (expected != null && actual != null)
			diffs = equalObjects(expected, actual);
		else if (expected != null && actual == null)
			diffs = "Expected non null actual, but found null";
		else
			diffs = "Expected null actual, but found '" + asString(actual) + "'";
		if (diffs == null)
			return null;
		String message = cntxt + ": " + diffs;
		if (!justCheck)
			fail(message);
		return message;
	}
}
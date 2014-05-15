// OS_STATUS: public
package com.tesora.dve.variable;

import java.util.HashSet;
import java.util.Set;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PEException;

public class VariableValueConverter {

	private static final long NANOS_PER_SECOND = 1000000000;

	public static int toInternalInteger(final String value) throws PEException {
		try {
			return Integer.parseInt(value);
		} catch (Exception e) {
			throw new PEException("Invalid integer value '" + value + "'");
		}
	}
	
	public static int toInternalInteger(final String value, int min, int max) throws PEException {
		int ret = toInternalInteger(value);
		
		if(ret < min || ret > max)
			throw new PEException("Value must be in the range " + min + " to " + max);
		
		return ret;
	}
	
	public static long secondsToNanos(final String value) throws PEException {
		try {
			return Math.round(Double.parseDouble(value) * NANOS_PER_SECOND);
		} catch (Exception e) {
			throw new PEException("Invalid floating point value '" + value + "'");
		}
	}

	@SuppressWarnings("serial")
	private final static Set<String> trueMap = new HashSet<String>(){{ add("true"); add("yes"); add("on"); add("1"); }};
	@SuppressWarnings("serial")
	private final static Set<String> falseMap = new HashSet<String>(){{ add("false"); add("no"); add("off"); add("0"); }};

	public static boolean toInternalBoolean(String value) throws PEException {
		String lc = value.trim().toLowerCase();
		if (trueMap.contains(lc))
			return true;
		if (falseMap.contains(lc))
			return false;
		throw new PEException("Invalid boolean value '" + value + "'");
	}

	public static String toExternalString(boolean v) {
		return (v ? PEConstants.YES : PEConstants.NO);
	}

}

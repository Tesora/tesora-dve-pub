// OS_STATUS: public
package com.tesora.dve.common;

public final class PELongUtils {

	private PELongUtils() {
	}

	public static Long decode(String str) throws NumberFormatException {
		int radix = 10;
		int index = 0;
		boolean negative = false;
		Long result;

		if (str.length() == 0)
			throw new NumberFormatException("Zero length string");
		char firstChar = str.charAt(0);
		// Handle sign, if present
		if (firstChar == '-') {
			negative = true;
			index++;
		} else if (firstChar == '+')
			index++;

		// Handle radix specifier, if present
		if (str.startsWith("0x", index) || str.startsWith("0X", index)) {
			index += 2;
			radix = 16;
		}

		if (str.startsWith("-", index) || str.startsWith("+", index))
			throw new NumberFormatException("Sign character in wrong position");

		try {
			result = Long.valueOf(str.substring(index), radix);
			result = negative ? Long.valueOf(-result.longValue()) : result;
		} catch (NumberFormatException e) {
			// If number is Long.MIN_VALUE, we'll end up here. The next line
			// handles this case, and causes any genuine format error to be
			// rethrown.
			String constant = negative ? ("-" + str.substring(index))
					: str.substring(index);
			result = Long.valueOf(constant, radix);
		}
		return result;
	}

}

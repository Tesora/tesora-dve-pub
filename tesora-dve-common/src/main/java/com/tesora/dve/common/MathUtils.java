// OS_STATUS: public
package com.tesora.dve.common;

import java.util.Collection;

public final class MathUtils {

	public static Long sum(final Collection<Long> values) {
		Long sum = 0l;
		for (final Long value : values) {
			sum += value;
		}

		return sum;
	}

	public static double mean(final Collection<Long> values) {
		final double total = sum(values);
		return total / values.size();
	}

	public static double round(final double value, final int numDecimals) {
		final double factor = Math.pow(10.0, numDecimals);
		return Math.round(value * factor) / factor;
	}

}

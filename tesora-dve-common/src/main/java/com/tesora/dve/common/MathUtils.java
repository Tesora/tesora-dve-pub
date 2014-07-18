package com.tesora.dve.common;

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

	public static boolean isInteger(final double value) {
		return value == Math.rint(value);
	}

}

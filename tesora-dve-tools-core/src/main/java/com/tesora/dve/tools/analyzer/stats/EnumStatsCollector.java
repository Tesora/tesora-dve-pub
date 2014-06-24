package com.tesora.dve.tools.analyzer.stats;

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

import java.io.PrintStream;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public class EnumStatsCollector<E extends Enum<E>> {
	EnumMap<E, Long> sampleCounts;
	long totalOccurances = 0L;

	public EnumStatsCollector(Class<E> clazz) {
		this.sampleCounts = new EnumMap<E, Long>(clazz);
		for (final E key : EnumSet.allOf(clazz)) {
			this.sampleCounts.put(key, 0L);
		}
	}

	public void increment(E key) {
		this.sample(key);
	}

	public void sample(E key) {
		this.sample(key, 1L);
	}

	public void sample(E key, long occurances) {
		if (occurances <= 0) {
			throw new IllegalArgumentException("Occurance count must be greater than zero");
		}

		this.totalOccurances += occurances;

		final long existing = sampleCounts.get(key);
		sampleCounts.put(key, occurances + existing);
	}

	public long getTotalOccurances() {
		return totalOccurances;
	}

	public long getOccurances(E key) {
		return sampleCounts.get(key);
	}

	public double getPercentage(E key) {
		if (totalOccurances == 0L) {
			return 0.0d;
		}

		final long occurred = sampleCounts.get(key);
		return ((double) occurred) / totalOccurances;
	}

	public Set<E> getKeys() {
		return sampleCounts.keySet();
	}

	public void printTo(PrintStream outputStream) {
		this.printTo("TOTAL : %s\n", "    %s : %s\n", outputStream);
	}

	public void printTo(String titleFormat, String detailFormat, PrintStream outputStream) {
		outputStream.printf(titleFormat, totalOccurances);
		for (final Map.Entry<E, Long> entry : sampleCounts.entrySet()) {
			outputStream.printf(detailFormat, entry.getKey(), entry.getValue());
		}
	}
}

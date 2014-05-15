// OS_STATUS: public
package com.tesora.dve.tools.analyzer.stats;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class IntegerHistogram {
	SortedMap<Integer, Long> sampleCounts = new TreeMap<>();

	long totalOccurances = 0L;

	long totalValues = 0L;
	long totalValuesSquared = 0L;

	Integer minimum;
	Integer maximum;

	public void sample(Integer key) {
		this.sample(key, 1L);
	}

	public void sample(Integer key, long occurances) {
		if (occurances <= 0) {
			throw new IllegalArgumentException("Occurance count must be greater than zero");
		}
		if (minimum == null) {
			minimum = key;
		} else if (key.compareTo(minimum) < 0) {
			minimum = key;
		}

		if (maximum == null) {
			maximum = key;
		} else if (key.compareTo(maximum) > 0) {
			maximum = key;
		}

		this.totalOccurances += occurances;
		totalValues += occurances * key;
		totalValuesSquared += occurances * (key * key);

		final Long existing = sampleCounts.get(key);
		if (existing == null) {
			sampleCounts.put(key, occurances);
		} else {
			sampleCounts.put(key, occurances + existing);
		}
	}

	public Integer getMinimum() {
		return this.minimum;
	}

	public Double getAverage() {
		if (this.totalOccurances != 0) {
			return ((1.0d) * this.totalValues) / totalOccurances;
		}

		return null;
	}

	public Double getStandardDeviation() {
		if (this.totalOccurances != 0) {
			return Math.sqrt(((1.0d * totalOccurances * totalValuesSquared) - (1.0d * totalValues * totalValues))
					/ (1.0d * totalOccurances * (totalOccurances - 1.0d)));
		}

		return null;
	}

	public double findPercentile(int value) {
		long accum = 0L;
		for (final Map.Entry<Integer, Long> entry : sampleCounts.entrySet()) {
			if (value < entry.getKey()) {
				break;
			}
			accum += entry.getValue();
		}
		return (1.0d * accum) / totalOccurances;
	}

	public Integer getPercentile(double percentile) {
		if (this.totalOccurances == 0) {
			return null;
		}
		final long targetThresh = (long) (totalOccurances * percentile);
		long accum = 0L;
		for (final Map.Entry<Integer, Long> entry : sampleCounts.entrySet()) {
			accum += entry.getValue();
			if (accum >= targetThresh) {
				return entry.getKey();
			}
		}

		return sampleCounts.lastKey();
	}

	public Integer getMaximum() {
		return this.maximum;
	}

	public long getTotalOccurances() {
		return totalOccurances;
	}

}

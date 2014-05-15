// OS_STATUS: public
package com.tesora.dve.server.statistics;

import com.tesora.dve.server.statistics.TimingValue;


public class RollingStatisticsAccumulator implements StatisticsAccumulator {
	
	final int quantumSize; // in ms

	final int accumSize;
	int[] statAccum;
	int[] statCount;
	
	long lastDatumQuantum;
	
	boolean debugMode = false;
	
	public RollingStatisticsAccumulator(int accumSize, int quantumSize) {
		this.accumSize = accumSize;
		this.quantumSize = quantumSize;
		statAccum = new int[accumSize];
		statCount = new int[accumSize];
		for (int i = 0; i < accumSize; ++i) {
			statAccum[i] = 0;
			statCount[i] = 0;
		}
		lastDatumQuantum = System.currentTimeMillis() / quantumSize;
	}
	
	@Override
	public void addDatum(int responseTimeMS) {
		long datumQuantum = transitionRollingCounter();
		if (debugMode)
			System.out.println("Datum position is " + datumQuantum % accumSize);
		statAccum[(int) (datumQuantum%accumSize)] += (responseTimeMS == 0) ? 1 : responseTimeMS;
		statCount[(int) (datumQuantum%accumSize)] += 1;
		lastDatumQuantum = datumQuantum;
	}

	@Override
	public float getAverageTime() {
		transitionRollingCounter();
		float avg = 0;
		int accum = 0;
		int count = 0;
		for (int i = 0; i < accumSize; ++i) {
			accum += statAccum[i];
			count += statCount[i];
		}
		if (count > 0)
			avg = accum / count;
		return avg / 1000;
	}
	
	@Override
	public float getAverageValue() {
		transitionRollingCounter();
		float accum = 0;

		for (int i = 0; i < accumSize; ++i) {
			if (statCount[i] > 0)
				accum += (statAccum[i] / statCount[i]);
		}
		return accum / accumSize;	
	}

	@Override
	public int getTransactionsPerSecond() {
		transitionRollingCounter();
		int count = 0;
		for (int i = 0; i < accumSize; ++i ) {
			count += statCount[i];
		}
		return count * 1000 / quantumSize;
	}

	public int getQuantumSize() {
		return quantumSize;
	}

	public int getAccumSize() {
		return accumSize;
	}
	
	long transitionRollingCounter() {
		long datumQuantum = System.currentTimeMillis() / quantumSize;
		if (debugMode)
			System.out.println(datumQuantum + " " + lastDatumQuantum);
		if (datumQuantum - lastDatumQuantum > accumSize)
			lastDatumQuantum = datumQuantum - accumSize;
		while (lastDatumQuantum < datumQuantum) {
			++lastDatumQuantum;
			if (debugMode)
				System.out.println("lastDatum position is " + lastDatumQuantum % accumSize);
			statAccum[(int) (lastDatumQuantum%accumSize)] = 0;
			statCount[(int) (lastDatumQuantum%accumSize)] = 0;
		}
		return datumQuantum;
	}

	@Override
	public boolean isDebugMode() {
		return debugMode;
	}

	@Override
	public void setDebugMode(boolean debugMode) {
		this.debugMode = debugMode;
	}
	
	@Override
	public TimingValue getTimingValue() {
		return new TimingValue(getAverageTime(), getTransactionsPerSecond());
	}
}

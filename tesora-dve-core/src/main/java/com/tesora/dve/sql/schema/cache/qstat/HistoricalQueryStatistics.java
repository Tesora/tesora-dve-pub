package com.tesora.dve.sql.schema.cache.qstat;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;


public class HistoricalQueryStatistics implements Serializable {

	private final LinkedList<HistoricalStatisticUnit> acc;
	private AtomicLong currentHistoricalAvg;

	// hardcoded for now, will make this configurable later
	private static final int maxWindows = 10;
	
	public HistoricalQueryStatistics() {
		this.acc = new LinkedList<HistoricalStatisticUnit>();
		this.currentHistoricalAvg = new AtomicLong(0);
	}

	public long getCurrentAvg() {
		return currentHistoricalAvg.get();
	}
	
	public void onHistoricalUnit(HistoricalStatisticUnit hsu) {
		acc.addFirst(hsu);
		if (acc.size() > maxWindows)
			acc.removeLast();
		long histacc = 0;
		for(HistoricalStatisticUnit i : acc) {
			histacc += i.getAvgRows();
		}
		currentHistoricalAvg.set(histacc / acc.size());
	}

}

package com.tesora.dve.sql.schema.cache.qstat;


// historical unit
public class HistoricalStatisticUnit implements StatisticMeasurement {
	
	private static final long serialVersionUID = 1L;
	// this will be 0 if the avg is 0 - which indicates the query returns no rows.  Score!
	private final long avg;
	private final long calls;
	
	public HistoricalStatisticUnit(long avg, long calls) {
		this.avg = avg;
		this.calls = calls;
	}

	@Override
	public long getAvgRows() {
		return avg;
	}

	@Override
	public long getCalls() {
		return calls;
	}
}
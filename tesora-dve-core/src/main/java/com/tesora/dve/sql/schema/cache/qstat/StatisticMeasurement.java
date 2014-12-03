package com.tesora.dve.sql.schema.cache.qstat;

import java.io.Serializable;

public interface StatisticMeasurement extends Serializable {
	
	public abstract long getAvgRows();
	
	// number of calls this unit represents
	public abstract long getCalls();
	
}
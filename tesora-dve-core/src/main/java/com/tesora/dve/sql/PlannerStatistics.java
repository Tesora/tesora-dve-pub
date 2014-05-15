// OS_STATUS: public
package com.tesora.dve.sql;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.exceptions.PEException;

public class PlannerStatistics {

	private static final PlannerStatistics instance = new PlannerStatistics();
	
	private final Map<PlannerStatisticType,AtomicLong> counters = new HashMap<PlannerStatisticType,AtomicLong>();
	
	private PlannerStatistics() {
		for(PlannerStatisticType pts : PlannerStatisticType.values()) {
			counters.put(pts, new AtomicLong(0));
		}
	}
	
	public static void increment(PlannerStatisticType pts) {
		instance.counters.get(pts).incrementAndGet();
	}
	
	public static long getCurrentValue(PlannerStatisticType pts) throws PEException {
		AtomicLong v = instance.counters.get(pts);
		
		if (v == null)
			throw new PEException("Unsupported PlannerStatisticType: " + pts.toString());
		
		return v.get();
	}
	
	public static void resetCurrentValue(PlannerStatisticType pts) throws PEException {
		AtomicLong v = instance.counters.get(pts);
		
		if (v == null)
			throw new PEException("Unsupported PlannerStatisticType: " + pts.toString());
		
		v.set(0);
	}
}

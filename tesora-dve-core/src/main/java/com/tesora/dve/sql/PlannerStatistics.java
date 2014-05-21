// OS_STATUS: public
package com.tesora.dve.sql;

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

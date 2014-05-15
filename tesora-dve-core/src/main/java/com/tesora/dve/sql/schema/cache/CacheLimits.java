// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;


import java.util.Arrays;

import com.tesora.dve.sql.schema.ConnectionContext;
import com.tesora.dve.sql.schema.SchemaVariables;

public class CacheLimits {

	private final int[] limits;
	
	public CacheLimits(ConnectionContext cc) {
		CacheSegment[] segments = CacheSegment.values();
		limits = new int[segments.length];
		for(CacheSegment cs : segments) {
			int value = SchemaVariables.getCacheLimit(cs, cc);
			limits[cs.ordinal()] = value; 
		}
	}
	
	private CacheLimits(CacheLimits other) {
		limits = Arrays.copyOf(other.limits, other.limits.length);
	}
	
	public int getLimit(CacheSegment cs) {
		return limits[cs.ordinal()];
	}
	
	public CacheLimits take(CacheSegment cs, int value) {
		CacheLimits out = new CacheLimits(this);
		out.limits[cs.ordinal()] = value;
		return out;
	}
	
}

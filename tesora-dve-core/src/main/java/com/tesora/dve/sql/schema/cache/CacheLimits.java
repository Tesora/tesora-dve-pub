package com.tesora.dve.sql.schema.cache;

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



import java.util.Arrays;

import com.tesora.dve.sql.schema.ConnectionContext;

public class CacheLimits {

	private final int[] limits;
	
	public CacheLimits(ConnectionContext cc) {
		CacheSegment[] segments = CacheSegment.values();
		limits = new int[segments.length];
		for(CacheSegment cs : segments) {
			int value = cs.getVariable().getValue(cc.getVariableSource()).intValue();
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

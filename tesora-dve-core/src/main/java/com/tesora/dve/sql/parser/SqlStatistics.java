package com.tesora.dve.sql.parser;

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
import com.tesora.dve.sql.statement.StatementType;

public final class SqlStatistics {

	private static final SqlStatistics instance = new SqlStatistics();
	
	private final AtomicLong[] counters = new AtomicLong[StatementType.values().length];
	
	private SqlStatistics() {
		for(StatementType st : StatementType.values()) {
			counters[st.ordinal()] = new AtomicLong(0);
		}
	}
	
	public static void incrementCounter(StatementType st) {
		instance.counters[st.ordinal()].incrementAndGet();
	}
	
	public static Map<StatementType,AtomicLong> getCurrentValues() {
		Map<StatementType,AtomicLong> out = new HashMap<StatementType,AtomicLong>();
		for(StatementType st : StatementType.values()) {
			out.put(st,new AtomicLong(instance.counters[st.ordinal()].get()));
		}
		return out;
	}	
	
	public static long getTotalValue() {
		long result = 0;
		for(StatementType st : StatementType.values())
			result += instance.counters[st.ordinal()].get();
		return result;
	}	
	
	public static long getCurrentValue(StatementType st) throws PEException {
		AtomicLong val = instance.counters[st.ordinal()];
		
		if(val == null)
			throw new PEException("Unsupported StatementType: " + st.toString());
		
		return val.get();
	}	

	public static void resetCurrentValue(StatementType st) throws PEException {
		AtomicLong val = instance.counters[st.ordinal()];

		if(val == null)
			throw new PEException("Unsupported StatementType: " + st.toString());
		
		val.set(0);
	}
	
	public static void resetAllValues() {
		for(StatementType st : StatementType.values())
			instance.counters[st.ordinal()].set(0);
	}
}

package com.tesora.dve.variables;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.util.Functional;

// all of our known variables
public class Variables {

	private static final ValueMetadata<Long> integralConverter = new IntegralValueConverter();
	
	public static final VariableHandler<Long> LARGE_INSERT_CUTOFF =
			new VariableHandler<Long>("large_insert_threshold",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL), 
					new Long(InvokeParser.defaultLargeInsertThreshold),
					true);

	
	private static final Map<String,VariableHandler<?>> allHandlers = buildAllHandlers();
		
	private static Map<String,VariableHandler<?>> buildAllHandlers() {
		ConcurrentHashMap<String,VariableHandler<?>> out = new ConcurrentHashMap<String,VariableHandler<?>>();
		add(LARGE_INSERT_CUTOFF,out);
		return out;
	}
	
	private static void add(VariableHandler<?> vh, ConcurrentHashMap<String,VariableHandler<?>> map) {
		map.put(vh.getName(), vh);
	}
	
	
	public static List<VariableHandler<?>> getSessionHandlers() {
		return Functional.select(allHandlers.values(), VariableHandler.isSessionPredicate);
	}
	
	public static List<VariableHandler<?>> getGlobalHandlers() {
		return Functional.select(allHandlers.values(), VariableHandler.isGlobalPredicate);
	}
	
	public static VariableHandler<?> lookup(String name, boolean except) throws PEException {
		VariableHandler<?> vh = allHandlers.get(name);
		if (vh == null && except)
			throw new PEException(String.format("No such variable: '%s'", name));
		return vh;
		
	}
	
	
}

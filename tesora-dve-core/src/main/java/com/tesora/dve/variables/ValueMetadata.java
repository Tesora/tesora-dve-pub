package com.tesora.dve.variables;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;

public abstract class ValueMetadata<Type> {

	public abstract Type convertToInternal(String in) throws PEException;

	public abstract String convertToExternal(Type in);

	public abstract ResultCollector getValueAsResult(Type in) throws PEException;

	public String validate(Type in) {
		return null;
	}
	
}

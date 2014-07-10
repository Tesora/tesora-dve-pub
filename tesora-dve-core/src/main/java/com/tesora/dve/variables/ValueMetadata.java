package com.tesora.dve.variables;

import java.sql.Types;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;

public abstract class ValueMetadata<Type> {

	public abstract Type convertToInternal(String in) throws PEException;

	public abstract String convertToExternal(Type in);

	public ResultCollector getValueAsResult(Type in) throws PEException {
		return ResultCollectorFactory.getInstance(Types.VARCHAR, convertToExternal(in));
	}

	public String validate(Type in) {
		return null;
	}
	
}

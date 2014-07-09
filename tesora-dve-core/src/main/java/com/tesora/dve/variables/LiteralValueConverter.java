package com.tesora.dve.variables;

import java.sql.Types;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;

public class LiteralValueConverter extends ValueMetadata<String> {

	@Override
	public String convertToInternal(String in) throws PEException {
		return in;
	}

	@Override
	public String convertToExternal(String in) {
		return in;
	}

	@Override
	public ResultCollector getValueAsResult(String in) throws PEException {
		return ResultCollectorFactory.getInstance(Types.VARCHAR, in);
	}

}

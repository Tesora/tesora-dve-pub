package com.tesora.dve.variables;

import java.sql.Types;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;

public class IntegralValueConverter extends ValueMetadata<Long> {

	@Override
	public Long convertToInternal(String in) throws PEException {
		try {
			return Long.parseLong(in);
		} catch (NumberFormatException nfe) {
			throw new PEException("Not an integral value: '" + in + "'");
		}
	}

	@Override
	public String convertToExternal(Long in) {
		return Long.toString(in);
	}

	@Override
	public ResultCollector getValueAsResult(Long in) throws PEException {
		return ResultCollectorFactory.getInstance(Types.INTEGER, in);
	}

}

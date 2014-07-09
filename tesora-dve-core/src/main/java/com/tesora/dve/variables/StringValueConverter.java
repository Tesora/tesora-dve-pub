package com.tesora.dve.variables;

import java.sql.Types;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;

public class StringValueConverter extends ValueMetadata<String> {

	@Override
	public String convertToInternal(String in) throws PEException {
		if (in == null) return null;
		return PEStringUtils.dequote(in);
	}

	@Override
	public String convertToExternal(String in) {
		if (in == null) return null;
		return String.format("'%s'",in);
	}

	@Override
	public ResultCollector getValueAsResult(String in) throws PEException {
		return ResultCollectorFactory.getInstance(Types.VARCHAR, in);
	}
	
}

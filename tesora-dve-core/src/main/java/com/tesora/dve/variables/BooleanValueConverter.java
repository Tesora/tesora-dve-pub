package com.tesora.dve.variables;

import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;

public class BooleanValueConverter extends ValueMetadata<Boolean> {

	@SuppressWarnings("serial")
	private final static Set<String> trueMap = new HashSet<String>(){{ add("true"); add("yes"); add("on"); add("1"); }};
	@SuppressWarnings("serial")
	private final static Set<String> falseMap = new HashSet<String>(){{ add("false"); add("no"); add("off"); add("0"); }};

	@Override
	public Boolean convertToInternal(String in) throws PEException {
		String lc = in.trim().toLowerCase();
		if (trueMap.contains(lc))
			return true;
		if (falseMap.contains(lc))
			return false;
		throw new PEException("Invalid boolean value '" + in + "'");
	}

	@Override
	public String convertToExternal(Boolean in) {
		return (Boolean.TRUE.equals(in) ? "1" : "0");
	}

	@Override
	public ResultCollector getValueAsResult(Boolean in) throws PEException {
		return ResultCollectorFactory.getInstance(Types.BOOLEAN, in);
	}

}

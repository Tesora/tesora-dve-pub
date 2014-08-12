package com.tesora.dve.variables;

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

import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;

public class BooleanValueConverter extends ValueMetadata<Boolean> {

	@SuppressWarnings("serial")
	private final static Set<String> trueMap = new HashSet<String>(){{ add("true"); add("yes"); add("on"); add("1"); }};
	@SuppressWarnings("serial")
	private final static Set<String> falseMap = new HashSet<String>(){{ add("false"); add("no"); add("off"); add("0"); }};

	@Override
	public Boolean convertToInternal(String varName, String in) throws PEException {
		String lc = in.trim().toLowerCase();
		if (trueMap.contains(lc))
			return true;
		if (falseMap.contains(lc))
			return false;
		throw new PEException(String.format("Invalid boolean value '%s' given for variable %s",in,varName));
	}

	@Override
	public String convertToExternal(Boolean in) {
		return (Boolean.TRUE.equals(in) ? "1" : "0");
	}

	@Override
	public ResultCollector getValueAsResult(Boolean in) throws PEException {
		return ResultCollectorFactory.getInstance(Types.BOOLEAN, in);
	}

	@Override
	public String getTypeName() {
		return "boolean";
	}

	@Override
	public String toRow(Boolean in) {
		return (Boolean.TRUE.equals(in) ? "YES" : "NO");
	}

}

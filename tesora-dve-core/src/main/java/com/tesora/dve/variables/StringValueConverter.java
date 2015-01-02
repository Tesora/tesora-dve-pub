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

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.variables.ResultCollector.ResultCollectorFactory;

public class StringValueConverter extends ValueMetadata<String> {

	@Override
	public String convertToInternal(String varName, String in) throws PEException {
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

	@Override
	public boolean isNumeric() {
		return false;
	}

	@Override
	public String getTypeName() {
		return "varchar";
	}

	@Override
	public String toRow(String in) {
		return PEStringUtils.dequote(in);
	}
	
}

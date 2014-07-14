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

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SQLMode;

// dang, I wanted to avoid having a bespoke class for this
public class SqlModeHandler extends VariableHandler<SQLMode> {

	private static final String DEFAULT_MODE_NAME = "default";
	private static final char VALUE_SEPARATOR = ',';

	public SqlModeHandler(String name, ValueMetadata<SQLMode> md,
			EnumSet<VariableScope> applies, SQLMode defaultOnMissing,
			EnumSet<VariableOption> options) {
		super(name, md, applies, defaultOnMissing, options);
	}

	@Override
	public void setSessionValue(VariableStoreSource conn, String value) throws PEException {
		final Set<String> dveRequiredModes = getSetValues(getGlobalValue(conn).toString());
		final Set<String> userDefinedModes = (!value.trim().equalsIgnoreCase(DEFAULT_MODE_NAME)) ? getSetValues(value) : new LinkedHashSet<String>();
		super.setSessionValue(conn, buildSetString(dveRequiredModes, userDefinedModes));
	}
	
	private  Set<String> getSetValues(final String setExpressionValue) {
		final String[] values = StringUtils.split(setExpressionValue, VALUE_SEPARATOR);
		final Set<String> uniqueValues = new LinkedHashSet<String>(values.length);
		for (final String value : values) {
			uniqueValues.add(value.trim().toUpperCase());
		}

		return uniqueValues;
	}

	private String buildSetString(final Set<String> dveRequiredModes, final Set<String> userDefinedModes) {
		final Set<String> setExpressionValues = new LinkedHashSet<String>();
		setExpressionValues.addAll(dveRequiredModes);
		setExpressionValues.addAll(userDefinedModes);

		return StringUtils.join(setExpressionValues, VALUE_SEPARATOR);
	}

}

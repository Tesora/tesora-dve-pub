// OS_STATUS: public
package com.tesora.dve.variable;

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

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public class SqlModeSessionVariableHandler extends DBSessionVariableHandler {

	public static final String VARIABLE_NAME = "sql_mode";

	private static final String DEFAULT_MODE_NAME = "default";
	private static final char VALUE_SEPARATOR = ',';

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		if (VARIABLE_NAME.equalsIgnoreCase(name)) {
			final Set<String> dveRequiredModes = getSetValues(getDefaultValue(ssCon));
			final Set<String> userDefinedModes = (!value.trim().equalsIgnoreCase(DEFAULT_MODE_NAME)) ? getSetValues(value) : new LinkedHashSet<String>();
			super.setValue(ssCon, name, buildSetString(dveRequiredModes, userDefinedModes));
		} else {
			throw new PECodingException(this.getClass().getSimpleName() + " called for " + name + " which it cannot handle");
		}
	}

	@Override
	public String getDefaultValue(SSConnection ssCon) throws PEException {
		return Singletons.require(HostService.class).getGlobalVariable(ssCon.getCatalogDAO(), getVariableName());
	}

	private static Set<String> getSetValues(final String setExpressionValue) {
		final String[] values = StringUtils.split(setExpressionValue, VALUE_SEPARATOR);
		final Set<String> uniqueValues = new LinkedHashSet<String>(values.length);
		for (final String value : values) {
			uniqueValues.add(value.trim().toUpperCase());
		}

		return uniqueValues;
	}

	private static String buildSetString(final Set<String> dveRequiredModes, final Set<String> userDefinedModes) {
		final Set<String> setExpressionValues = new LinkedHashSet<String>();
		setExpressionValues.addAll(dveRequiredModes);
		setExpressionValues.addAll(userDefinedModes);

		return StringUtils.join(setExpressionValues, VALUE_SEPARATOR);
	}

}

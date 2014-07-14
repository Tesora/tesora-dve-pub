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

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.exceptions.PEException;

public class GlobalConfigVariableHandler extends ConfigVariableHandler implements VariableConstants, Serializable {	
	
	private static final long serialVersionUID = 1L;

	@Override
	public void setValue(CatalogDAO c, String name, String value) throws PEException {
		// Check the type / and range of values here. If valid then the values are persisted
		// and then onValueChange is called to propagate the values to the rest of the system


		super.setValue(c, name, value);
	}
	
	private void checkInteger(String value, String name, int min, boolean allowZero) throws PEException {
		int intValue = VariableValueConverter.toInternalInteger(value);
		if (intValue < min && (allowZero && intValue != 0)) {
			throw new PEException("Invalid value for " + name + " (value must be " + (allowZero ? "0 or " : "") + "greater than " + min + ")");
		}
	}

	private <T extends Enum<T>> void checkString(final String value, final String name, final Class<T> allowedValueEnum) throws PEException {
		try {
			Enum.valueOf(allowedValueEnum, value);
		} catch (final IllegalArgumentException e) {
			throw new PEException("Invalid value for " + name + " (allowed values are " + StringUtils.join(allowedValueEnum.getEnumConstants(), ", ") + ")");
		}
	}

	private static String normalizeStringValue(final String value) {
		return value.trim().toUpperCase();
	}

	@Override
	public void onValueChange(String name, String value) throws PEException {
		/*
        BootstrapHostService host = Singletons.require(BootstrapHostService.class);

		} else if (SLOW_QUERY_LOG.equalsIgnoreCase(name)) {
			resetSlowQueryLogVarState(value);
		} else if (TEMPLATE_MODE.equalsIgnoreCase(name)) {
			resetTemplateMode(value);
		*/
	}

	public static void initializeGlobalVariables(Map<String, String> variables) throws PEException {
		/*
		BootstrapHostService host = Singletons.require(BootstrapHostService.class);

		host.setTableCleanupInterval(Integer.valueOf(variables.get(ADAPTIVE_CLEANUP_INTERVAL_NAME)), false);

		for (final Map.Entry<String, String> variable : variables.entrySet()) {
			final String variableName = variable.getKey();
			final String variableValue = variable.getValue();

			if (variableName.equalsIgnoreCase(SLOW_QUERY_LOG)) {
				resetSlowQueryLogVarState(variableValue);
			} else if (variableName.equalsIgnoreCase(TEMPLATE_MODE)) {
				resetTemplateMode(variableValue);
			}

			SchemaVariables.initializeGlobalVariableCache(variableName,
					SchemaVariables.convertToNativeType(variableName, variableValue));
		}
		*/
	}
}
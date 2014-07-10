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
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SlowQueryLogger;
import com.tesora.dve.server.global.BootstrapHostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;

public class GlobalConfigVariableHandler extends ConfigVariableHandler implements GlobalConfigVariableConstants, Serializable {	
	
	private static final long serialVersionUID = 1L;

	@Override
	public void setValue(CatalogDAO c, String name, String value) throws PEException {
		// Check the type / and range of values here. If valid then the values are persisted
		// and then onValueChange is called to propagate the values to the rest of the system

		if (STATISTICS_INTERVAL.equalsIgnoreCase(name)) {
			checkInteger(value, name, 1000, true);
		} else if (ADAPTIVE_CLEANUP_INTERVAL.equalsIgnoreCase(name)) {
			checkInteger(value, name, 1000, true);
		} else if (SQL_LOGGING.equalsIgnoreCase(name)) {
			VariableValueConverter.toInternalBoolean(value);
		} else if (BALANCE_PERSISTENT_GROUPS.equalsIgnoreCase(name)) {
			VariableValueConverter.toInternalBoolean(value);
		} else if(MAX_CACHED_PLAN_LITERALS.equalsIgnoreCase(name)) {
			VariableValueConverter.toInternalInteger(value);
		} else if(PE_MYSQL_EMULATE_LIMIT.equalsIgnoreCase(name)) {
			VariableValueConverter.toInternalInteger(value);
		} else if(INNODB_LOCK_WAIT_TIMEOUT.equalsIgnoreCase(name)) {
			VariableValueConverter.toInternalInteger(value);
		} else if(WAIT_TIMEOUT.equalsIgnoreCase(name)) {
			VariableValueConverter.toInternalInteger(value);
		} else if(REDIST_MAX_COLUMNS.equalsIgnoreCase(name)) {
			VariableValueConverter.toInternalInteger(value);
		} else if(REDIST_MAX_SIZE.equalsIgnoreCase(name)) {
			VariableValueConverter.toInternalInteger(value);
		} else if(GROUP_CONCAT_MAX_LEN.equalsIgnoreCase(name)) {
			VariableValueConverter.toInternalInteger(value);
		} else if (TEMPLATE_MODE.equalsIgnoreCase(name)) {
			checkString(normalizeStringValue(value), name, TemplateMode.class);
		}

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
        BootstrapHostService host = Singletons.require(BootstrapHostService.class);

		if (ADAPTIVE_CLEANUP_INTERVAL.equalsIgnoreCase(name)) {
			host.setTableCleanupInterval(VariableValueConverter.toInternalInteger(value), true);
		} else if(MAX_CACHED_PLAN_LITERALS.equalsIgnoreCase(name)) {
			SchemaVariables.setCacheVariable(name, SchemaVariables.convertToNativeType(name, value));
		} else if (LARGE_INSERT_THRESHOLD.equalsIgnoreCase(name)) {
			SchemaVariables.setCacheVariable(LARGE_INSERT_THRESHOLD, SchemaVariables.convertToNativeType(LARGE_INSERT_THRESHOLD, value));
		} else if (SQL_LOGGING.equalsIgnoreCase(name)) {
			InvokeParser.enableSqlLogging(VariableValueConverter.toInternalBoolean(value));
		} else if (SLOW_QUERY_LOG.equalsIgnoreCase(name)) {
			resetSlowQueryLogVarState(value);
		} else if (LONG_PLAN_STEP_TIME.equalsIgnoreCase(name)) {
			SchemaVariables.setCacheVariable(LONG_PLAN_STEP_TIME, SchemaVariables.convertToNativeType(LONG_PLAN_STEP_TIME, value));
		} else if (BALANCE_PERSISTENT_GROUPS.equalsIgnoreCase(name)) {
			SchemaVariables.setCacheVariable(BALANCE_PERSISTENT_GROUPS, SchemaVariables.convertToNativeType(BALANCE_PERSISTENT_GROUPS, value));
		} else if (BALANCE_PERSISTENT_GROUPS_PREFIX.equalsIgnoreCase(name)) {
			SchemaVariables.setCacheVariable(BALANCE_PERSISTENT_GROUPS_PREFIX, value);
		} else if (STEPWISE_STATISTICS.equalsIgnoreCase(name)) {
			SchemaVariables.setCacheVariable(STEPWISE_STATISTICS, SchemaVariables.convertToNativeType(name, value));
		} else if (LONG_QUERY_TIME.equalsIgnoreCase(name)) {
			SchemaVariables.setCacheVariable(LONG_QUERY_TIME, SchemaVariables.convertToNativeType(LONG_QUERY_TIME, value));
		} else if (TEMPLATE_MODE.equalsIgnoreCase(name)) {
			resetTemplateMode(value);
		} else {
			CacheSegment segment = CacheSegment.lookupSegment(name.toLowerCase(Locale.ENGLISH));
			if (segment != null) {
				int limit = VariableValueConverter.toInternalInteger(value);
				SchemaSourceFactory.setCacheSegmentLimit(segment,limit);
			}
		}
	}

	public static void initializeGlobalVariables(Map<String, String> variables) throws PEException {
		BootstrapHostService host = Singletons.require(BootstrapHostService.class);

		host.setTableCleanupInterval(Integer.valueOf(variables.get(ADAPTIVE_CLEANUP_INTERVAL)), false);

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
	}

	private static void resetSlowQueryLogVarState(final String value) throws PEException {
		final Boolean slowQueryLogTrigger = (Boolean) SchemaVariables.convertToNativeType(SLOW_QUERY_LOG, value);
		SchemaVariables.setCacheVariable(SLOW_QUERY_LOG, slowQueryLogTrigger);
		SlowQueryLogger.enableSlowQueryLogger(slowQueryLogTrigger);
	}

	private static void resetTemplateMode(final String value) throws PEException {
		final String modeName = (String) SchemaVariables.convertToNativeType(TEMPLATE_MODE, normalizeStringValue(value));
		SchemaVariables.setCacheVariable(TEMPLATE_MODE, modeName);
	}
}
// OS_STATUS: public
package com.tesora.dve.sql.schema;

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

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.variable.SchemaVariableConstants;
import com.tesora.dve.variable.VariableAccessor;
import com.tesora.dve.variable.VariableScopeKind;
import com.tesora.dve.variable.VariableValueConverter;

public class SchemaVariables implements SchemaVariableConstants {

	public static final VariableAccessor SHOW_METADATA_EXTENSIONS = new VariableAccessor(VariableScopeKind.SESSION,SHOW_METADATA_EXTENSIONS_NAME);
	public static final VariableAccessor SELECT_LIMIT = new VariableAccessor(VariableScopeKind.SESSION, SQL_SELECT_LIMIT_NAME);
	public static final VariableAccessor CACHED_PLAN_LITERALS_MAX =
			new VariableAccessor(VariableScopeKind.DVE, MAX_CACHED_LITERALS_NAME);
	
	public static final VariableAccessor FOREIGN_KEY_CHECKS = new VariableAccessor(VariableScopeKind.SESSION, FOREIGN_KEY_CHECKS_NAME);
	public static final VariableAccessor STORAGE_ENGINE = new VariableAccessor(VariableScopeKind.SESSION, STORAGE_ENGINE_NAME);
	public static final VariableAccessor OMIT_DIST_COMMENTS = new VariableAccessor(VariableScopeKind.SESSION, OMIT_DIST_COMMENTS_NAME);
	
	public static final VariableAccessor SQL_MODE = new VariableAccessor(VariableScopeKind.SESSION, "sql_mode");

	public static final VariableAccessor COLLATION_CONNECTION = new VariableAccessor(VariableScopeKind.SESSION, COLLATION_CONNECTION_NAME);
	public static final VariableAccessor CHARACTER_SET_CLIENT = new VariableAccessor(VariableScopeKind.SESSION, CHARACTER_SET_CLIENT_NAME);
	
	public static final VariableAccessor REPL_INSERT_ID = new VariableAccessor(VariableScopeKind.SESSION, REPL_SLAVE_INSERT_ID);
	public static final VariableAccessor LARGE_INSERT_CUTOFF = new VariableAccessor(VariableScopeKind.DVE, LARGE_INSERT_THRESHOLD);
	public static final VariableAccessor EMULATE_MYSQL_LIMIT = new VariableAccessor(VariableScopeKind.DVE, LIMIT_ORDERBY_EMULATION);
	public static final VariableAccessor STEPWISE_STATISTICS = new VariableAccessor(VariableScopeKind.DVE, STEPWISE_STATISTICS_NAME);
	public static final VariableAccessor COST_BASED_PLANNING = new VariableAccessor(VariableScopeKind.DVE, CARDINALITY_COSTING_NAME);
	
	public static final VariableAccessor TEMPLATE_MODE = new VariableAccessor(VariableScopeKind.DVE, TEMPLATE_MODE_NAME);
	
	public static final VariableAccessor REPL_TIMESTAMP = new VariableAccessor(VariableScopeKind.SESSION, REPL_SLAVE_TIMESTAMP);
	
	public static final VariableAccessor SLOW_QUERY_LOG = new VariableAccessor(VariableScopeKind.DVE, SLOW_QUERY_LOG_NAME);
	public static final VariableAccessor LONG_PLAN_STEP_TIME = new VariableAccessor(VariableScopeKind.DVE, LONG_PLAN_STEP_TIME_NAME);
	public static final VariableAccessor LONG_QUERY_TIME = new VariableAccessor(VariableScopeKind.DVE, LONG_QUERY_TIME_NAME);

	public static final VariableAccessor BALANCE_PERSISTENT_GROUPS = new VariableAccessor(VariableScopeKind.DVE, BALANCE_PERSISTENT_GROUPS_NAME);
	public static final VariableAccessor BALANCE_PERSISTENT_GROUPS_PREFIX = new VariableAccessor(VariableScopeKind.DVE, BALANCE_PERSISTENT_GROUPS_PREFIX_NAME);

	public static final VariableAccessor DEFAULT_DYNAMIC_POLICY = new VariableAccessor(VariableScopeKind.DVE, DEFAULT_DYNAMIC_POLICY_NAME);
	
	private static final VariableAccessor[] globals = new VariableAccessor[] {
		LARGE_INSERT_CUTOFF,
		SLOW_QUERY_LOG, LONG_PLAN_STEP_TIME, LONG_QUERY_TIME,
		EMULATE_MYSQL_LIMIT, CACHED_PLAN_LITERALS_MAX,
		BALANCE_PERSISTENT_GROUPS, BALANCE_PERSISTENT_GROUPS_PREFIX,
		DEFAULT_DYNAMIC_POLICY, STEPWISE_STATISTICS,
		COST_BASED_PLANNING, TEMPLATE_MODE
	};
	
	// keep a local cache so we don't have to keep going back to the catalog
	private static final ConcurrentHashMap<String, Object> values = new ConcurrentHashMap<String,Object>();

	private static final String defaultLargeInsertThreshold = Long.toString(InvokeParser.defaultLargeInsertThreshold);
	private static final String defaultDynamicPolicyName =  OnPremiseSiteProvider.DEFAULT_POLICY_NAME;
	
	private static Object getCachedVariableValue(VariableAccessor va) {
		if (va.getScopeKind() != VariableScopeKind.DVE)
			throw new SchemaException(Pass.SECOND,
					"Invalid cached variable request - attempt to access variable of type " + va.getScopeKind());
		return values.get(va.getVariableName());
	}
	
	private static <T> T getCachedVariableValueAs(ConnectionContext sc, VariableAccessor accessor, String defVal,
			final Class<T> type) {
		final T cachedValue = type.cast(getCachedVariableValue(accessor));

		if (cachedValue == null) {
			final String value = getVariableOnCacheMiss(sc, accessor, defVal);
			if (value != null) {
				try {
					return type.cast(setGetCachedValue(accessor.getVariableName(),
							convertToNativeType(accessor.getVariableName(), value)));
				} catch (final PEException e) {
					throw new SchemaException(Pass.PLANNER, "Unable to convert value for " + accessor.getVariableName());
				}
			}

			throw new SchemaException(Pass.PLANNER, "Unable to determine value for " + accessor.getVariableName());
		}

		return cachedValue;
	}

	private static <T extends Enum<T>> T getCachedVariableValueAs(ConnectionContext sc, VariableAccessor accessor, T defVal,
			final Class<T> type) {
		return Enum.valueOf(type, getCachedVariableValueAs(sc, accessor, defVal.toString(), String.class));
	}

	private static String getVariableOnCacheMiss(ConnectionContext sc, VariableAccessor accessor, String defVal) {
		if (sc != null) {
			try {
				final String catalogValue = sc.getVariableValue(accessor);
				if (catalogValue != null) {
					return catalogValue;
				}
			} catch (PEException e) {
				return defVal;
			}
		}

		return defVal;
	}

	public static boolean isShowMetadataExtensions(SchemaContext cntxt) throws PEException {
		if (cntxt == null) return false;
		String value = cntxt.getConnection().getVariableValue(SHOW_METADATA_EXTENSIONS);
		return VariableValueConverter.toInternalBoolean(value);
	}
	
	public static boolean isOmitDistComments(SchemaContext cntxt) {
		if (cntxt == null) return false;
		try {
			String value = cntxt.getConnection().getVariableValue(OMIT_DIST_COMMENTS);
			if (value == null) return false;
			return VariableValueConverter.toInternalBoolean(value);
		} catch (PEException pe) {
			return false;
		}
	}
	
	public static Long getSelectLimit(SchemaContext cntxt) throws PEException {
		String value = cntxt.getConnection().getVariableValue(SELECT_LIMIT);
		if (value == null) return null;
		if ("DEFAULT".equals(value.toUpperCase(Locale.ENGLISH))) return null;
		try {
			return Long.parseLong(value);
		} catch (NumberFormatException nfe) {
			throw new PEException("Unable to determine select limit", nfe);
		}
	}
	
	public static int getCacheLimit(CacheSegment segment, ConnectionContext cc) {
		try {
			String value = cc.getVariableValue(segment.getAccessor());
			if (value == null) return segment.getDefaultValue();
			return Integer.parseInt(value);
		} catch (Throwable t) {
			throw new SchemaException(Pass.PLANNER, "Unable to determine limit for " + segment.getVariableName(),t);
		}
	}	
	
	public static boolean hasForeignKeyChecks(SchemaContext cntxt) {
		try {
			String value = cntxt.getConnection().getVariableValue(FOREIGN_KEY_CHECKS);
			if (value == null) return true; // default
			int ival = Integer.parseInt(value);
			return ival == 1;
		} catch (Throwable t) {
			throw new SchemaException(Pass.PLANNER, "Unable to determine value for " + FOREIGN_KEY_CHECKS_NAME,t);
		}
	}

	public static String getStorageEngine(SchemaContext cntxt) {
		return getSessionValue(cntxt,STORAGE_ENGINE);
	}

	public static Long getReplInsertId(SchemaContext sc) {
		try {
			String value = sc.getConnection().getVariableValue(REPL_INSERT_ID);
			if (value == null) return null;
			Long lval = Long.parseLong(value);
			if (lval.longValue() == 0) return null;
			return lval;
		} catch (Throwable t) {
			throw new SchemaException(Pass.PLANNER, "Unable to determine insert id",t);
		}
	}
	
	public static String getCollationConnection(SchemaContext cntxt) {
		return getSessionValue(cntxt,COLLATION_CONNECTION);
 	}
	
	public static String getCharacterSetClient(SchemaContext cntxt) {
		return getSessionValue(cntxt,CHARACTER_SET_CLIENT);
	}
	
	private static String getSessionValue(SchemaContext cntxt, VariableAccessor accessor) {
		try {
			return cntxt.getConnection().getVariableValue(accessor);
		} catch (Throwable t) {
			throw new SchemaException(Pass.PLANNER, "Unable to determine value for " + accessor.getVariableName(),t);
		}
	}
	
	public static int getLargeInsertThreshold(SchemaContext sc) {
		Integer val = getCachedVariableValueAs(sc.getConnection(), LARGE_INSERT_CUTOFF, defaultLargeInsertThreshold,
				Integer.class);
		return val.intValue();
	}
	
	public static int getLiteralsCutoff(SchemaContext sc) {
		return getCachedVariableValueAs(sc.getConnection(), CACHED_PLAN_LITERALS_MAX, "100", Integer.class);
	}
	
	public static boolean slowQueryLogEnabled(ConnectionContext cc) {
		return getCachedVariableValueAs(cc, SLOW_QUERY_LOG, "0", Boolean.class);
	}
	
	public static long getLongPlanStepTimeNanos(ConnectionContext cc) {
		return getCachedVariableValueAs(cc, LONG_PLAN_STEP_TIME, "0", Long.class);
	}
	
	public static long getLongQueryTimeNanos(ConnectionContext cc) {
		return getCachedVariableValueAs(cc, LONG_QUERY_TIME, "0", Long.class);
	}
	
	public static boolean getStepwiseStatistics(ConnectionContext cc) {
		return getCachedVariableValueAs(cc, STEPWISE_STATISTICS, "false", Boolean.class);
	}
	
	public static boolean getCostBasedPlanning(ConnectionContext cc) {
		return getCachedVariableValueAs(cc, COST_BASED_PLANNING, "true", Boolean.class);
	}
	
	public static TemplateMode getTemplateMode(ConnectionContext cc) {
		return getCachedVariableValueAs(cc, TEMPLATE_MODE, TemplateMode.REQUIRED,
				TemplateMode.class);
	}
	
	public static boolean emulateMysqlLimit(SchemaContext sc) {
		return getCachedVariableValueAs(sc.getConnection(), EMULATE_MYSQL_LIMIT, "0", Boolean.class);
	}

	public static boolean getBalancePersisentGroups(SchemaContext sc) {
		return getCachedVariableValueAs(sc.getConnection(), BALANCE_PERSISTENT_GROUPS, "0", Boolean.class);
	}
	
	public static String getBalancePersistentGroupsPrefix(SchemaContext sc) {
		return getCachedVariableValueAs(sc.getConnection(), BALANCE_PERSISTENT_GROUPS_PREFIX, null, String.class);
	}
	
	public static String getDefaultPolicyName(SchemaContext sc) {
		return getCachedVariableValueAs(sc.getConnection(), DEFAULT_DYNAMIC_POLICY, defaultDynamicPolicyName, String.class);
	}
	
	public static Long getReplTimestamp(SchemaContext sc) {
		try {
			String value = sc.getConnection().getVariableValue(REPL_TIMESTAMP);
			if (value == null) return null;
			Long lval = Long.parseLong(value);
			if (lval.longValue() == 0) return null;
			return lval;
		} catch (Throwable t) {
			throw new SchemaException(Pass.PLANNER, "Unable to determine timestamp",t);
		}
	}
	
	public static SQLMode getSQLMode(SchemaContext sc) {
		try {
			return new SQLMode(sc.getConnection().getVariableValue(SQL_MODE));
		} catch (PEException pe) {
			throw new SchemaException(Pass.NORMALIZE, "Unable to obtain sql_mode");
		}
	}

	public static void initializeGlobalVariableCache(final String name, final Object value) {
		if (isGlobalVariable(name)) {
			setCacheVariable(name, value);
		}
	}

	public static void setCacheVariable(final String name, final Object value) {
		if (value != null) {
			setGetCachedValue(name, value);
		}
	}

	public static Object convertToNativeType(final String name, final String value) throws PEException {
		if (value != null) {

			if (name.equalsIgnoreCase(LONG_PLAN_STEP_TIME_NAME)
					|| name.equalsIgnoreCase(LONG_QUERY_TIME_NAME)) {
				return VariableValueConverter.secondsToNanos(value);
			} else if (name.equalsIgnoreCase(SLOW_QUERY_LOG_NAME)
					|| name.equalsIgnoreCase(LIMIT_ORDERBY_EMULATION)
					|| name.equalsIgnoreCase(BALANCE_PERSISTENT_GROUPS_NAME)
					|| name.equalsIgnoreCase(STEPWISE_STATISTICS_NAME)
					|| name.equalsIgnoreCase(CARDINALITY_COSTING_NAME)) {
				return VariableValueConverter.toInternalBoolean(value);
			} else if (name.equalsIgnoreCase(LARGE_INSERT_THRESHOLD)
					|| name.equalsIgnoreCase(MAX_CACHED_LITERALS_NAME)) {
				return VariableValueConverter.toInternalInteger(value);
			}

			return value;
		}

		return null;
	}

	private static <T> T setGetCachedValue(final String name, final T value) {
		values.put(name, value);
		return value;
	}

	private static boolean isGlobalVariable(final String name) {
		for (final VariableAccessor variable : globals) {
			if (variable.getVariableName().equalsIgnoreCase(name)) {
				return true;
			}
		}

		return false;
	}
}

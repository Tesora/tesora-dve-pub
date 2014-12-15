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
import java.util.Locale;

import com.tesora.dve.charset.*;
import com.tesora.dve.clock.TimingServiceConfiguration;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.AutoIncrementTracker;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.db.mysql.MySQLTransactionIsolation;
import com.tesora.dve.errmap.AvailableErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.server.connectionmanager.ConnectionSemaphore;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.SlowQueryLogger;
import com.tesora.dve.server.global.BootstrapHostService;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.global.MySqlPortalService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.TimestampVariableUtils;
import com.tesora.dve.sql.schema.SQLMode;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.schema.modifiers.EngineTableModifier.EngineTag;
import com.tesora.dve.variable.VariableConstants;
import com.tesora.dve.variables.BooleanValueConverter.BooleanToStringConverter;

public class KnownVariables implements VariableConstants {

	public static final ValueMetadata<Long> integralConverter = new IntegralValueConverter();
	public static final ValueMetadata<Boolean> booleanConverter = new BooleanValueConverter(BooleanToStringConverter.YES_NO_CONVERTER);
	public static final ValueMetadata<Double> floatingPointConverter = new FloatingPointValueConverter();
	public static final ValueMetadata<String> stringConverter = new StringValueConverter();
	public static final ValueMetadata<String> literalConverter = new LiteralValueConverter();

	public static final EnumSet<VariableScopeKind> globalScope = EnumSet.of(VariableScopeKind.GLOBAL);
	public static final EnumSet<VariableScopeKind> sessionScope = EnumSet.of(VariableScopeKind.SESSION);
	public static final EnumSet<VariableScopeKind> bothScope = EnumSet.of(VariableScopeKind.GLOBAL, VariableScopeKind.SESSION);

	// normally emulated variables are just passed through
	public static final EnumSet<VariableOption> emulated = EnumSet.of(VariableOption.EMULATED, VariableOption.PASSTHROUGH);
	public static final EnumSet<VariableOption> emulatedOnly = EnumSet.of(VariableOption.EMULATED);
	public static final EnumSet<VariableOption> dveOnly = EnumSet.noneOf(VariableOption.class);
	public static final EnumSet<VariableOption> nullable = EnumSet.of(VariableOption.NULLABLE);
	public static final EnumSet<VariableOption> readonly = EnumSet.of(VariableOption.READONLY);

	@SuppressWarnings("rawtypes")
	static final ValueMetadata[] defaultConverters = new ValueMetadata[] {
			integralConverter,
			booleanConverter,
			floatingPointConverter,
			stringConverter,
			literalConverter
	};

	public static final VariableHandler<Long> LARGE_INSERT_CUTOFF =
			new VariableHandler<Long>(LARGE_INSERT_THRESHOLD_NAME,
					integralConverter,
					globalScope,
					InvokeParser.defaultLargeInsertThreshold,
					dveOnly,
					"How many characters to process at a time for long insert commands");
	public static final VariableHandler<Long> CACHED_PLAN_LITERALS_MAX =
			new VariableHandler<Long>(MAX_CACHED_LITERALS_NAME,
					integralConverter,
					globalScope,
					100L,
					dveOnly,
					"Maximum number of literals per cached plan in plan cache");
	public static final VariableHandler<Boolean> SLOW_QUERY_LOG =
			new VariableHandler<Boolean>(SLOW_QUERY_LOG_NAME,
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					emulatedOnly) {

				@Override
				public void onGlobalValueChange(Boolean newValue) throws PEException {
					SlowQueryLogger.enableSlowQueryLogger(newValue);
				}
			};
	public static final VariableHandler<Double> LONG_PLAN_STEP_TIME =
			new VariableHandler<Double>(LONG_PLAN_STEP_TIME_NAME,
					floatingPointConverter,
					globalScope,
					new Double(3.0),
					dveOnly,
					"Minimum per step trigger duration for slow query log");
	public static final VariableHandler<Double> LONG_QUERY_TIME =
			new VariableHandler<Double>(LONG_QUERY_TIME_NAME,
					floatingPointConverter,
					bothScope,
					new Double(10.0),
					emulatedOnly);
	public static final VariableHandler<Boolean> EMULATE_MYSQL_LIMIT =
			new VariableHandler<Boolean>(LIMIT_ORDERBY_EMULATION_NAME,
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					dveOnly,
					"Emulate mysql's limit behavior (add order by primary key if unspecified)");
	public static final VariableHandler<Boolean> BALANCE_PERSISTENT_GROUPS =
			new VariableHandler<Boolean>(BALANCE_PERSISTENT_GROUPS_NAME,
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					dveOnly,
					"Enable balanced group allocation");
	public static final VariableHandler<String> BALANCE_PERSISTENT_GROUPS_PREFIX =
			new VariableHandler<String>(BALANCE_PERSISTENT_GROUPS_PREFIX_NAME,
					stringConverter,
					globalScope,
					"",
					dveOnly,
					"Balanced group prefix");
	public static final VariableHandler<String> DYNAMIC_POLICY =
			new VariableHandler<String>(DYNAMIC_POLICY_NAME,
					new StringValueConverter() {

						@Override
						public String convertToInternal(String varName, String in) throws PEException {
							String raw = stringConverter.convertToInternal(varName, in);
							CatalogDAO c = CatalogDAOFactory.newInstance();
							try {
								c.findDynamicPolicy(raw, true);
							} finally {
								c.close();
							}
							return raw;
						}

					},
					bothScope,
					null,
					nullable,
					"Dynamic policy name");
	public static final VariableHandler<Boolean> STEPWISE_STATISTICS =
			new VariableHandler<Boolean>(STEPWISE_STATISTICS_NAME,
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					dveOnly,
					"Track per step execution statistics in plan cache");
	public static final VariableHandler<Boolean> COST_BASED_PLANNING =
			new VariableHandler<Boolean>(CARDINALITY_COSTING_NAME,
					booleanConverter,
					globalScope,
					Boolean.TRUE,
					dveOnly,
					"Enable cost based planning");
	public static final VariableHandler<TemplateMode> TEMPLATE_MODE =
			new VariableHandler<TemplateMode>(TEMPLATE_MODE_NAME,
					new EnumValueConverter<TemplateMode>(TemplateMode.values()),
					globalScope,
					TemplateMode.REQUIRED,
					dveOnly,
					"Specify behavior on missing distribution metadata injection");
	public static final VariableHandler<Boolean> SHOW_METADATA_EXTENSIONS =
			new VariableHandler<Boolean>(SHOW_METADATA_EXTENSIONS_NAME,
					booleanConverter,
					sessionScope,
					Boolean.FALSE,
					dveOnly,
					"Show DVE specific metadata for show commands");
	public static final VariableHandler<Boolean> OMIT_DIST_COMMENTS =
			new VariableHandler<Boolean>(OMIT_DIST_COMMENTS_NAME,
					booleanConverter,
					sessionScope,
					Boolean.FALSE,
					dveOnly,
					"Inhibit distribution declarations on show create table");
	public static final VariableHandler<Long> SELECT_LIMIT =
			new VariableHandler<Long>(SQL_SELECT_LIMIT_NAME,
					new IntegralValueConverter() {
						@Override
						public Long convertToInternal(String varName, String in) throws PEException {
							return super.convertToInternal(varName, in);
						}

						@Override
						public String convertToExternal(Long in) {
							if (in == null)
								return VariableHandler.DEFAULT_KEYWORD;
							return Long.toString(in);
						}

					},
					sessionScope,
					null,
					EnumSet.of(VariableOption.NULLABLE, VariableOption.EMULATED)) {

			};
	public static final VariableHandler<Boolean> FOREIGN_KEY_CHECKS =
			new VariableHandler<Boolean>(FOREIGN_KEY_CHECKS_NAME,
					booleanConverter,
					bothScope,
					Boolean.TRUE,
					emulated);
	public static final VariableHandler<EngineTag> STORAGE_ENGINE =
			new VariableHandler<EngineTag>(STORAGE_ENGINE_NAME,
					new EnumValueConverter<EngineTag>(EngineTag.values()) {
						@Override
						public String convertToExternal(EngineTag in) {
							return String.format("'%s'", in.getSQL());
						}

						@Override
						public EngineTag convertToInternal(String varName, String in) throws PEException {
							String deq = PEStringUtils.dequote(in);
							String uc = deq.toUpperCase(Locale.ENGLISH);
							return super.convertToInternal(varName, uc);
						}

					},
					bothScope,
					EngineTag.INNODB,
					emulated);
	public static final VariableHandler<Long> REPL_INSERT_ID =
			new VariableHandler<Long>(REPL_SLAVE_INSERT_ID_NAME,
					integralConverter,
					sessionScope,
					null,
					EnumSet.of(VariableOption.NULLABLE),
					"Replication slave last insert id");
	public static final VariableHandler<Long> TIMESTAMP = new VariableHandler<Long>("timestamp",
			/* As of MySQL 5.6.4, timestamp is a DOUBLE. */ new IntegralValueConverter() {
		@Override
		public Long convertToInternal(String varName, String in) throws PEException {
			return super.convertToInternal(varName, in);
		}
	},
			sessionScope,
			0L,
			EnumSet.of(VariableOption.EMULATED)) {
		@Override
		public Long getValue(VariableStoreSource source, VariableScopeKind vs) {
			final Long currentSessionValue = super.getValue(source, vs);
			if (currentSessionValue == 0) {
				return TimestampVariableUtils.getCurrentSystemTime();
			}
			return currentSessionValue;
		}
	};
	public static final VariableHandler<Long> REPL_TIMESTAMP =
			new VariableHandler<Long>(REPL_SLAVE_TIMESTAMP_NAME,
					integralConverter,
					sessionScope,
					null,
					EnumSet.of(VariableOption.NULLABLE),
					"Replication slave last timestamp");
	public static final VariableHandler<SQLMode> SQL_MODE =
			new VariableHandler<SQLMode>("sql_mode",
					new ValueMetadata<SQLMode>() {

						@Override
						public SQLMode convertToInternal(String varName, String in)
								throws PEException {
							String raw = stringConverter.convertToInternal(varName, in);
							return new SQLMode(raw);
						}

						@Override
						public String convertToExternal(SQLMode in) {
							return String.format("'%s'", in.toString());
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
						public String toRow(SQLMode in) {
							return in.toString();
						}

					},
					bothScope,
					new SQLMode("NO_ENGINE_SUBSTITUTION"),
					emulated) {

				@Override
				public void onGlobalValueChange(SQLMode newValue) throws PEException {
					super.onGlobalValueChange(newValue);
				}

				@Override
				public void setGlobalValue(VariableStoreSource conn, String value) throws PEException {
					final SQLMode newModes = combineSQLMode(conn, value);
					super.setGlobalValue(conn, newModes.toString());
				}

				@Override
				public void onSessionValueChange(VariableStoreSource conn, SQLMode newValue) throws PEException {
					super.onSessionValueChange(conn, newValue);
				}

				@Override
				public void setSessionValue(VariableStoreSource conn, String value) throws PEException {
					final SQLMode newModes = combineSQLMode(conn, value);
					super.setSessionValue(conn, newModes.toString());
				}

				/**
				 * Combine given SQL MODEs with the ones required by DVE.
				 * Make sure the global values required by DVE are always
				 * included.
				 */
				private SQLMode combineSQLMode(final VariableStoreSource conn, final String value) throws PEException {
					final SQLMode globalValues = getGlobalValue(conn);
					final String raw = stringConverter.convertToInternal(getName(), value);

					if (DEFAULT_KEYWORD.equalsIgnoreCase(raw)) {
						// return the required global values
						return globalValues;
					}

					// append new modes to the defaults
					final SQLMode myValues = new SQLMode(raw);
					final SQLMode combined = globalValues.add(myValues);
					return combined;
				}
			};
	public static final VariableHandler<String> COLLATION_CONNECTION =
			new VariableHandler<String>(COLLATION_CONNECTION_NAME,
					new LiteralValueConverter() {

						@Override
						public String convertToInternal(String varName, String in) throws PEException {
							Singletons.require(HostService.class).getDBNative().assertValidCollation(in);
							return in;
						}

					},
					bothScope,
					"utf8_general_ci",
					emulated) {
		
		@Override
		public void setGlobalValue(VariableStoreSource conn, String value) throws PEException {
			final NativeCharSet parentCharSet =
					Singletons.require(NativeCharSetCatalog.class).findCharSetByCollation(PEStringUtils.dequote(value));
			if (parentCharSet != null) {
				super.setGlobalValue(conn, value);
				final String parentCharSetValue = parentCharSet.getName();
				if (!parentCharSetValue.equals(CHARACTER_SET_CONNECTION.getGlobalValue(conn))) {
					CHARACTER_SET_CONNECTION.setGlobalValue(conn, parentCharSetValue);
				}
			} else {
				throw new SchemaException(new ErrorInfo(AvailableErrors.UNKNOWN_COLLATION, value));
			}
		}

		@Override
		public void setSessionValue(VariableStoreSource conn, String value) throws PEException {
			final NativeCharSet parentCharSet =
					Singletons.require(NativeCharSetCatalog.class).findCharSetByCollation(PEStringUtils.dequote(value));
			if (parentCharSet != null) {
				super.setSessionValue(conn, value);
				final String parentCharSetValue = parentCharSet.getName();
				if (!parentCharSetValue.equals(CHARACTER_SET_CONNECTION.getSessionValue(conn))) {
					CHARACTER_SET_CONNECTION.setSessionValue(conn, parentCharSetValue);
				}
			} else {
				throw new SchemaException(new ErrorInfo(AvailableErrors.UNKNOWN_COLLATION, value));
			}
		}
	};
	public static final VariableHandler<String> CHARACTER_SET_CONNECTION =
			new VariableHandler<String>("character_set_connection",
			stringConverter,
			bothScope,
			"utf8",
			emulated) {
		
		@Override
		public void setGlobalValue(VariableStoreSource conn, String value) throws PEException {
			final NativeCollation defaultCollation =
					Singletons.require(NativeCollationCatalog.class).findDefaultCollationForCharSet(PEStringUtils.dequote(value));
			if (defaultCollation != null) {
				super.setGlobalValue(conn, value);
				final String defaultCollationValue = defaultCollation.getName();
				if (!defaultCollationValue.equals(COLLATION_CONNECTION.getGlobalValue(conn))) {
					COLLATION_CONNECTION.setGlobalValue(conn, defaultCollationValue);
				}
			} else {
				throw new SchemaException(new ErrorInfo(AvailableErrors.UNKNOWN_CHARACTER_SET, value));
			}
		}

		@Override
		public void setSessionValue(VariableStoreSource conn, String value) throws PEException {
			final NativeCollation defaultCollation =
					Singletons.require(NativeCollationCatalog.class).findDefaultCollationForCharSet(PEStringUtils.dequote(value));
			if (defaultCollation != null) {
				super.setSessionValue(conn, value);
				final String defaultCollationValue = defaultCollation.getName();
				if (!defaultCollationValue.equals(COLLATION_CONNECTION.getSessionValue(conn))) {
					COLLATION_CONNECTION.setSessionValue(conn, defaultCollationValue);
				}
			} else {
				throw new SchemaException(new ErrorInfo(AvailableErrors.UNKNOWN_CHARACTER_SET, value));
			}
		}
		
	};
	public static final VariableHandler<String> CHARACTER_SET_RESULTS =
			new VariableHandler<String>("character_set_results",
			stringConverter,
			bothScope,
			"utf8",
			EnumSet.of(VariableOption.EMULATED, VariableOption.PASSTHROUGH, VariableOption.NULLABLE));
	public static final VariableHandler<NativeCharSet> CHARACTER_SET_CLIENT =
			new VariableHandler<NativeCharSet>(CHARACTER_SET_CLIENT_NAME,
					new ValueMetadata<NativeCharSet>() {

						@Override
						public NativeCharSet convertToInternal(String varName,
								String in) throws PEException {
							final NativeCharSet charSet = MysqlNativeCharSetCatalog.DEFAULT_CATALOG.findCharSetByName(in);
							if (charSet == null) {
								throw new PEException(String.format("Unsupported charset for '%s': %s", varName, in));
							}
							return charSet;
						}

						@Override
						public String convertToExternal(NativeCharSet in) {
							return in.getName();
						}

						@Override
						public String toRow(NativeCharSet in) {
							return in.getName();
						}

						@Override
						public boolean isNumeric() {
							return false;
						}

						@Override
						public String getTypeName() {
							return "varchar";
						}

					},
					bothScope,
					MysqlNativeCharSet.UTF8,
					emulated) {

				public void onSessionValueChange(VariableStoreSource conn, NativeCharSet in) throws PEException {
					if (conn instanceof SSConnection) {
						SSConnection ssconn = (SSConnection) conn;
						ssconn.setClientCharSet(in);
					}
					super.onSessionValueChange(conn, in);
				}

			};
	public static final VariableHandler<Long> CACHE_LIMIT =
			new VariableHandler<Long>("cache_limit",
					integralConverter,
					globalScope,
					10000L,
					dveOnly,
					"Maximum uncategorized cache size (in entries)") {

				@Override
				public void onGlobalValueChange(Long newValue) throws PEException {
					SchemaSourceFactory.setCacheSegmentLimit(CacheSegment.lookupSegment(getName()), newValue.intValue());
				}
			};
	public static final VariableHandler<Long> SCOPE_CACHE_LIMIT =
			new VariableHandler<Long>("scope_cache_limit",
					integralConverter,
					globalScope,
					10000L,
					dveOnly,
					"Maximum scope cache size (in entries)") {
				@Override
				public void onGlobalValueChange(Long newValue) throws PEException {
					SchemaSourceFactory.setCacheSegmentLimit(CacheSegment.lookupSegment(getName()), newValue.intValue());
				}

			};
	public static final VariableHandler<Long> TENANT_CACHE_LIMIT =
			new VariableHandler<Long>("tenant_cache_limit",
					integralConverter,
					globalScope,
					500L,
					dveOnly,
					"Maximum tenant cache size (in entries)") {
				@Override
				public void onGlobalValueChange(Long newValue) throws PEException {
					SchemaSourceFactory.setCacheSegmentLimit(CacheSegment.lookupSegment(getName()), newValue.intValue());
				}

			};
	public static final VariableHandler<Long> TABLE_CACHE_LIMIT =
			new VariableHandler<Long>("table_cache_limit",
					integralConverter,
					globalScope,
					400L,
					dveOnly,
					"Maximum table cache size (in entries)") {
				@Override
				public void onGlobalValueChange(Long newValue) throws PEException {
					SchemaSourceFactory.setCacheSegmentLimit(CacheSegment.lookupSegment(getName()), newValue.intValue());
				}

			};
	public static final long DEFAULT_PLAN_CACHE_LIMIT = 400L;
	public static final VariableHandler<Long> PLAN_CACHE_LIMIT =
			new VariableHandler<Long>("plan_cache_limit",
					integralConverter,
					globalScope,
					DEFAULT_PLAN_CACHE_LIMIT,
					dveOnly,
					"Maximum plan cache size (in entries)") {
				@Override
				public void onGlobalValueChange(Long newValue) throws PEException {
					SchemaSourceFactory.setCacheSegmentLimit(CacheSegment.lookupSegment(getName()), newValue.intValue());
				}

			};
	public static final VariableHandler<Long> RAW_PLAN_CACHE_LIMIT =
			new VariableHandler<Long>("raw_plan_cache_limit",
					integralConverter,
					globalScope,
					0L,
					dveOnly,
					"Maximum raw plan cache size (in entries)") {
				@Override
				public void onGlobalValueChange(Long newValue) throws PEException {
					SchemaSourceFactory.setCacheSegmentLimit(CacheSegment.lookupSegment(getName()), newValue.intValue());
				}

			};
	public static final VariableHandler<Long> TEMPLATE_CACHE_LIMIT =
			new VariableHandler<Long>("template_cache_limit",
					integralConverter,
					globalScope,
					100L,
					dveOnly,
					"Maximum template cache size (in entries)") {
				@Override
				public void onGlobalValueChange(Long newValue) throws PEException {
					SchemaSourceFactory.setCacheSegmentLimit(CacheSegment.lookupSegment(getName()), newValue.intValue());
				}

			};
	// we don't push down the max_prepared_stmt_count so as to avoid issues with redist
	public static final VariableHandler<Long> MAX_PREPARED_STMT_COUNT =
			new VariableHandler<Long>("max_prepared_stmt_count",
					integralConverter,
					globalScope,
					16382L,
					emulatedOnly) {
				@Override
				public void onGlobalValueChange(Long newValue) throws PEException {
					SchemaSourceFactory.setCacheSegmentLimit(CacheSegment.lookupSegment(getName()), newValue.intValue());
				}
			};
	public static final VariableHandler<Long> REDIST_MAX_COLUMNS =
			new VariableHandler<Long>("redist_max_columns",
					integralConverter,
					bothScope,
					1000L,
					dveOnly,
					"Maximum number of columns per redistribution insert");
	public static final VariableHandler<Long> REDIST_MAX_SIZE =
			new VariableHandler<Long>("redist_max_size",
					integralConverter,
					bothScope,
					16000000L,
					dveOnly,
					"Maximum byte size per redistribution insert");
	public static final VariableHandler<Long> WAIT_TIMEOUT =
			new VariableHandler<Long>("wait_timeout",
			integralConverter,
			bothScope,
			28800L,
			emulatedOnly,
			"The number of seconds the server waits for activity on a noninteractive frontend connection before closing it");
	public static final VariableHandler<Long> BACKEND_WAIT_TIMEOUT =
			new VariableHandler<Long>("backend_wait_timeout",
					integralConverter,
					bothScope,
					28800L,
					EnumSet.of(VariableOption.READONLY),
					"The number of seconds the server waits for activity on a backend connection before closing it");
	public static final VariableHandler<Boolean> AUTOCOMMIT =
			new VariableHandler<Boolean>("autocommit",
					booleanConverter,
					bothScope,
					Boolean.TRUE,
					emulated) {

				@Override
				public void onSessionValueChange(VariableStoreSource store, Boolean newValue) throws PEException {
					((SSConnection) store).setAutoCommitMode(newValue.booleanValue());
				}

				@Override
				public String getSessionAssignmentClause(String value) {
					// does not seem correct
					return "autocommit = 1";
				}
			};
	public static final VariableHandler<String> TIME_ZONE =
			new VariableHandler<String>("time_zone",
					stringConverter,
					bothScope,
					"+00:00",
					emulated);
	public static final VariableHandler<String> PERSISTENT_GROUP =
			new VariableHandler<String>("persistent_group",
					stringConverter,
					bothScope,
					null,
					EnumSet.of(VariableOption.NULLABLE),
					"Default persistent group") {

				@Override
				public void setSessionValue(VariableStoreSource conn, String newValue) throws PEException {
					if (conn instanceof SSConnection) {
						SSConnection scon = (SSConnection) conn;
						scon.getCatalogDAO().findPersistentGroup(newValue,
								/* check existence */ true);
					}
					super.setSessionValue(conn, newValue);
				}

			};
	public static final VariableHandler<String> GROUP_SERVICE =
			new VariableHandler<String>("group_service",
					new StringValueConverter() {
						@Override
						public String convertToInternal(String varName, String in) throws PEException {
							String raw = super.convertToInternal(varName, in);
							if (!GroupManager.isValidServiceType(raw))
								throw new PEException("Value '" + raw + "' is not valid for variable 'group_service'");
							return raw;
						}
					},
					globalScope,
					"Localhost",
					dveOnly,
					"Enable support for multiple DVE servers");
	public static final VariableHandler<Long> MAX_CONCURRENT =
			new VariableHandler<Long>("max_concurrent",
					new BoundedIntegralConverter(1L, 5000L),
					globalScope,
					100L,
					dveOnly,
					"Maximum number of active concurrent connections") {

				@Override
				public void onGlobalValueChange(Long newValue) throws PEException {
					MySqlPortalService portal = Singletons.lookup(MySqlPortalService.class);
					if (portal != null)
						portal.setMaxConcurrent(newValue.intValue());
				}

			};
	public static final VariableHandler<Boolean> HAVE_SSL =
			new VariableHandler<Boolean>("have_ssl",
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY));
	// make this nullable to avoid some issues around initialization
	public static final VariableHandler<String> VERSION_COMMENT =
			new VariableHandler<String>("version_comment",
					stringConverter,
					bothScope,
					null,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY, VariableOption.NULLABLE)) {

				@Override
				public String getValue(VariableStoreSource source, VariableScopeKind vs) {
					return Singletons.require(HostService.class).getServerVersionComment();
				}

				@Override
				public String getDefaultOnMissing() {
					return null;
				}

			};
	public static final VariableHandler<String> VERSION =
			new VariableHandler<String>("version",
					stringConverter,
					bothScope,
					"5.5.10",
					EnumSet.of(VariableOption.READONLY, VariableOption.EMULATED));
	public static final VariableHandler<Boolean> SQL_LOGGING =
			new VariableHandler<Boolean>("sql_logging",
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					dveOnly,
					"Enable sql log") {

				@Override
				public void onGlobalValueChange(Boolean newValue) throws PEException {
					InvokeParser.enableSqlLogging(newValue.booleanValue());
				}
			};
	public static final VariableHandler<Long> MAX_ALLOWED_PACKET =
			new VariableHandler<Long>("max_allowed_packet",
					integralConverter,
					globalScope,
					16777216L,
					emulated);
	public static final VariableHandler<String> CHARACTER_SET_DATABASE =
			new VariableHandler<String>("character_set_database",
					new LiteralValueConverter() {

						@Override
						public String convertToInternal(String varName, String in) throws PEException {
							Singletons.require(HostService.class).getDBNative().assertValidCharacterSet(in);
							return in;
						}
					},
					bothScope,
					"utf8",
					emulated);
	public static final VariableHandler<String> COLLATION_DATABASE =
			new VariableHandler<String>("collation_database",
					new LiteralValueConverter() {

						@Override
						public String convertToInternal(String varName, String in) throws PEException {
							Singletons.require(HostService.class).getDBNative().assertValidCollation(in);
							return in;
						}
					},
					bothScope,
					"utf8_general_ci",
					emulated);
	public static final VariableHandler<Boolean> ERROR_MIGRATOR =
			new VariableHandler<Boolean>("verbose_error_handling",
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					dveOnly,
					"Enable location information in error messages");
	public static final VariableHandler<Long> INNODB_STATS_TRANSIENT_SAMPLE_PAGES =
			new VariableHandler<Long>("innodb_stats_transient_sample_pages",
			new BoundedIntegralConverter(1L, Long.MAX_VALUE),
			globalScope,
			8L,
			emulatedOnly);

	@SuppressWarnings("rawtypes")
	static VariableHandler[] namedHandlers = new VariableHandler[] {
			LARGE_INSERT_CUTOFF,
			CACHED_PLAN_LITERALS_MAX,
			SLOW_QUERY_LOG,
			LONG_PLAN_STEP_TIME,
			LONG_QUERY_TIME,
			EMULATE_MYSQL_LIMIT,
			BALANCE_PERSISTENT_GROUPS,
			BALANCE_PERSISTENT_GROUPS_PREFIX,
			DYNAMIC_POLICY,
			STEPWISE_STATISTICS,
			COST_BASED_PLANNING,
			TEMPLATE_MODE,
			SHOW_METADATA_EXTENSIONS,
			OMIT_DIST_COMMENTS,
			SELECT_LIMIT,
			FOREIGN_KEY_CHECKS,
			STORAGE_ENGINE,
			REPL_INSERT_ID,
			TIMESTAMP,
			REPL_TIMESTAMP,
			SQL_MODE,
			CHARACTER_SET_CONNECTION,
			CHARACTER_SET_RESULTS,
			COLLATION_CONNECTION,
			CHARACTER_SET_CLIENT,
			CACHE_LIMIT,
			SCOPE_CACHE_LIMIT,
			TENANT_CACHE_LIMIT,
			TABLE_CACHE_LIMIT,
			PLAN_CACHE_LIMIT,
			RAW_PLAN_CACHE_LIMIT,
			TEMPLATE_CACHE_LIMIT,
			MAX_PREPARED_STMT_COUNT,
			REDIST_MAX_COLUMNS,
			REDIST_MAX_SIZE,
			WAIT_TIMEOUT,
			BACKEND_WAIT_TIMEOUT,
			AUTOCOMMIT,
			TIME_ZONE,
			PERSISTENT_GROUP,
			GROUP_SERVICE,
			MAX_CONCURRENT,
			HAVE_SSL,
			VERSION,
			VERSION_COMMENT,
			SQL_LOGGING,
			MAX_ALLOWED_PACKET,
			CHARACTER_SET_DATABASE,
			COLLATION_DATABASE,
			ERROR_MIGRATOR,
			INNODB_STATS_TRANSIENT_SAMPLE_PAGES
	};

	@SuppressWarnings("rawtypes")
	static final VariableHandler[] unnamedHandlers = new VariableHandler[] {
			new VariableHandler<Long>("group_concat_max_len",
					integralConverter,
					bothScope,
					1024L,
					emulated) {

			},
			new VariableHandler<String>("hostname",
					stringConverter,
					globalScope,
					"",
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)) {

				@Override
				public String getValue(VariableStoreSource source, VariableScopeKind vs) {
					return Singletons.require(HostService.class).getHostName();
				}
			},
			new VariableHandler<Long>("auto_increment_increment",
					integralConverter,
					bothScope,
					1L,
					emulated),
			new VariableHandler<String>("character_set_server",
					stringConverter,
					bothScope,
					"utf8",
					emulated),
			new VariableHandler<String>("collation_server",
					stringConverter,
					bothScope,
					"utf8_general_ci",
					emulated),
			new VariableHandler<Long>(ADAPTIVE_CLEANUP_INTERVAL_NAME,
					new BoundedIntegralConverter(-1L, 10000L),
					globalScope,
					10000L,
					dveOnly,
					"Table garbage collector execution period (multitenant mode)") {

				public void onGlobalValueChange(Long newValue) throws PEException {
					Singletons.require(BootstrapHostService.class).setTableCleanupInterval(newValue.intValue(), true);
				}
			},
			new VariableHandler<Long>(STATISTICS_INTERVAL_NAME,
					new BoundedIntegralConverter(-1L, 1000L),
					globalScope,
					0L,
					dveOnly,
					"Statistics rollup interval"),
			new VariableHandler<Long>("max_connections",
					new BoundedIntegralConverter(1L, 5000L),
					globalScope,
					1000L,
					dveOnly,
					"Maximum number of concurrent connections") {

				public void onGlobalValueChange(Long newValue) throws PEException {
					SSConnection.setMaxConnections(newValue.intValue());
				}
			},
			new VariableHandler<Long>("connection_mem_requirement",
					integralConverter,
					globalScope,
					1000000L,
					dveOnly,
					"Minimum amount of memory to reserve for new connections") {
				public void onGlobalValueChange(Long newValue) throws PEException {
					ConnectionSemaphore.adjustMinMemory(newValue.intValue());
				}
			},
			new VariableHandler<Boolean>("debug_context",
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					dveOnly,
					"Enable debug tracing") {
				public void onGlobalValueChange(Boolean newValue) throws PEException {
                    //deprecated, but I left the variable for backwards compatibility. -sgossard
//					PEThreadContext.setEnabled(newValue.booleanValue());
				}
			},
			new VariableHandler<Long>("last_insert_id",
					integralConverter,
					sessionScope,
					0L,
					emulatedOnly),
			new VariableHandler<String>("charset",
					literalConverter,
					sessionScope,
					"utf8",
					emulatedOnly),
			new VariableHandler<Boolean>("sql_auto_is_null",
					booleanConverter,
					bothScope,
					Boolean.FALSE,
					emulated),
			new VariableHandler<MySQLTransactionIsolation>("tx_isolation",
					new EnumValueConverter<MySQLTransactionIsolation>(MySQLTransactionIsolation.values()),
					bothScope,
					MySQLTransactionIsolation.READ_COMMITTED,
					emulated),
			new VariableHandler<Boolean>("performance_trace",
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					dveOnly,
					"Enable performance tracing") {

				@Override
				public void onGlobalValueChange(Boolean newValue) throws PEException {
					TimingServiceConfiguration timingConfiguration = Singletons.lookup(TimingServiceConfiguration.class);
					if (timingConfiguration != null)
						timingConfiguration.setTimingEnabled(newValue.booleanValue());
				}
			},
			new VariableHandler<Long>("rand_seed1",
					integralConverter,
					sessionScope,
					0L,
					emulated),
			new VariableHandler<Long>("rand_seed2",
					integralConverter,
					sessionScope,
					0L,
					emulated),
			new VariableHandler<Long>("insert_id",
					integralConverter,
					sessionScope,
					0L,
					emulated),
			new VariableHandler<Long>(INNODB_LOCK_WAIT_TIMEOUT_NAME,
					integralConverter,
					bothScope,
					3L,
					emulated),
			new VariableHandler<Boolean>("have_innodb",
					booleanConverter,
					bothScope,
					Boolean.TRUE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("unique_checks",
					booleanConverter,
					bothScope,
					Boolean.TRUE,
					emulated),
			new VariableHandler<Boolean>("big_tables",
					booleanConverter,
					sessionScope,
					Boolean.FALSE,
					emulated),
			new VariableHandler<Long>(AUTO_INCREMENT_MIN_BLOCK_SIZE,
					new BoundedIntegralConverter(1L, null),
					globalScope,
					10L,
					dveOnly,
					"Minimum autoincrement allocation block size") {

				public void onGlobalValueChange(Long newValue) throws PEException {
					AutoIncrementTracker.setMinBlockSize(newValue.intValue());
				}
			},
			new VariableHandler<Long>(AUTO_INCREMENT_PREFETCH_THRESHOLD,
					new BoundedIntegralConverter(0L, 100L),
					globalScope,
					50L,
					dveOnly,
					"Request a new autoincrement block when this percentage of the previous block is used") {
				public void onGlobalValueChange(Long newValue) throws PEException {
					AutoIncrementTracker.setPrefetchThreshold(newValue.intValue());
				}

			},
			new VariableHandler<Long>(AUTO_INCREMENT_MAX_BLOCK_SIZE,
					integralConverter,
					globalScope,
					1000L,
					dveOnly,
					"Maximum autoincrement allocation block size") {
				public void onGlobalValueChange(Long newValue) throws PEException {
					AutoIncrementTracker.setMaxBlockSize(newValue.intValue());
				}

			},
			new VariableHandler<Long>("sql_quote_show_create",
					new BoundedIntegralConverter(-1L, 2L),
					bothScope,
					1L,
					emulated),
			new VariableHandler<Long>("sql_notes",
					new BoundedIntegralConverter(-1L, 2L),
					bothScope,
					1L,
					emulated),
			new VariableHandler<String>("init_connect",
					stringConverter,
					globalScope,
					"",
					emulated),
			new VariableHandler<Long>("interactive_timeout",
					new BoundedIntegralConverter(0L, null),
					bothScope,
					28800L,
					emulated),
			new VariableHandler<Long>("lower_case_table_names",
					new BoundedIntegralConverter(-1L, 3L),
					globalScope,
					1L,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Long>("net_buffer_length",
					new BoundedIntegralConverter(1023L, 1048577L),
					bothScope,
					16384L,
					emulatedOnly),
			new VariableHandler<Long>("net_write_timeout",
					new BoundedIntegralConverter(0L, null),
					bothScope,
					60L,
					emulated),
			new VariableHandler<String>("system_time_zone",
					stringConverter,
					globalScope,
					"EDT", // todo: change this
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Long>("query_cache_size",
					new BoundedIntegralConverter(-1L, 4294967296L), // maybe not right?
					globalScope,
					41943040L,
					emulatedOnly),
			new VariableHandler<Long>("query_cache_type",
					new BoundedIntegralConverter(-1L, 3L),
					bothScope,
					1L,
					emulatedOnly),
			new VariableHandler<Boolean>("have_dynamic_loading",
					booleanConverter,
					bothScope,
					Boolean.FALSE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<String>("version_compile_os",
					stringConverter,
					bothScope,
					"debian-linux-gnu",
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("have_geometry",
					booleanConverter,
					globalScope,
					Boolean.TRUE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Long>("myisam_sort_buffer_size",
					new BoundedIntegralConverter(4095L, Long.MAX_VALUE),
					bothScope,
					8388608L,
					emulated),
			new VariableHandler<Long>("sort_buffer_size",
					new BoundedIntegralConverter(32767L, Long.MAX_VALUE),
					bothScope,
					2097144L,
					emulated),
			new VariableHandler<Boolean>("lower_case_file_system",
					new BooleanValueConverter(BooleanToStringConverter.ON_OFF_CONVERTER),
					globalScope,
					Boolean.FALSE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("have_query_cache",
					booleanConverter,
					globalScope,
					Boolean.TRUE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("have_openssl" /* Alias for 'have_ssl'. */,
					booleanConverter,
					globalScope,
					HAVE_SSL.getDefaultOnMissing(),
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<ThreadHandlingMode>("thread_handling",
					new EnumValueConverter<ThreadHandlingMode>(ThreadHandlingMode.values()),
					globalScope,
					ThreadHandlingMode.ONE_THREAD_PER_CONNECTION,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("have_partitioning",
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<String>("time_format",
					stringConverter,
					globalScope,
					"%H:%i:%s",
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Long>("thread_concurrency",
					integralConverter,
					globalScope,
					10L,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("log_bin",
					new BooleanValueConverter(BooleanToStringConverter.ON_OFF_CONVERTER),
					globalScope,
					Boolean.FALSE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<String>("license",
					stringConverter,
					globalScope,
					"AGPL",
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<String>("innodb_version" /* Alias for 'version'. */,
					stringConverter,
					globalScope,
					VERSION.getDefaultOnMissing(),
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("ignore_builtin_innodb",
					new BooleanValueConverter(BooleanToStringConverter.ON_OFF_CONVERTER),
					globalScope,
					Boolean.FALSE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("have_profiling",
					booleanConverter,
					globalScope,
					Boolean.FALSE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<String>("datetime_format",
					stringConverter,
					globalScope,
					"%Y-%m-%d %H:%i:%s",
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<String>("date_format",
					stringConverter,
					globalScope,
					"%Y-%m-%d",
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<String>("character_set_system",
					stringConverter,
					globalScope,
					"utf8",
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Long>("div_precision_increment",
					new BoundedIntegralConverter(0L, 30L),
					bothScope,
					4L,
					emulated),
			new VariableHandler<Long>("default_week_format",
					new BoundedIntegralConverter(0L, 7L),
					bothScope,
					0L,
					emulated),
			new VariableHandler<String>("version_compile_machine",
					stringConverter,
					globalScope,
					"64-bit",
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("innodb_adaptive_flushing",
					new BooleanValueConverter(BooleanToStringConverter.ON_OFF_CONVERTER),
					globalScope,
					Boolean.TRUE,
					emulatedOnly),
			new VariableHandler<Long>("innodb_fast_shutdown",
					new BoundedIntegralConverter(0L, 2L),
					globalScope,
					1L,
					emulatedOnly),
			new VariableHandler<Long>("innodb_mirrored_log_groups",
					integralConverter,
					globalScope,
					1L,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<InnoDbStatsMethod>("innodb_stats_method",
					new EnumValueConverter<InnoDbStatsMethod>(InnoDbStatsMethod.values()),
					globalScope,
					InnoDbStatsMethod.NULLS_EQUAL,
					emulatedOnly),
			new VariableHandler<Long>("innodb_stats_sample_pages" /* Replaced by 'innodb_stats_transient_sample_pages' in  5.6. */,
					INNODB_STATS_TRANSIENT_SAMPLE_PAGES.getMetadata(),
					INNODB_STATS_TRANSIENT_SAMPLE_PAGES.getScopes(),
					INNODB_STATS_TRANSIENT_SAMPLE_PAGES.getDefaultOnMissing(),
					INNODB_STATS_TRANSIENT_SAMPLE_PAGES.getOptions()),
			new VariableHandler<Boolean>("innodb_table_locks",
					new BooleanValueConverter(BooleanToStringConverter.ON_OFF_CONVERTER),
					bothScope,
					Boolean.TRUE,
					emulated),
			new VariableHandler<Boolean>("myisam_use_mmap",
					new BooleanValueConverter(BooleanToStringConverter.ON_OFF_CONVERTER),
					globalScope,
					Boolean.FALSE,
					emulatedOnly),
			new VariableHandler<Long>("tmp_table_size",
					new BoundedIntegralConverter(1024L, Long.MAX_VALUE),
					bothScope,
					16777216L,
					emulated),
			new VariableHandler<Long>("protocol_version",
					integralConverter,
					globalScope,
					10L,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY)),
			new VariableHandler<Boolean>("skip_networking",
					new BooleanValueConverter(BooleanToStringConverter.ON_OFF_CONVERTER),
					globalScope,
					Boolean.FALSE,
					EnumSet.of(VariableOption.EMULATED, VariableOption.READONLY))
	};

}

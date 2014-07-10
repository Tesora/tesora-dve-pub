package com.tesora.dve.variables;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.schema.SQLMode;
import com.tesora.dve.sql.schema.modifiers.EngineTableModifier.EngineTag;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.variable.SchemaVariableConstants;

// all of our known variables
public class Variables implements SchemaVariableConstants {

	private static final ValueMetadata<Long> integralConverter = new IntegralValueConverter();
	private static final ValueMetadata<Boolean> booleanConverter = new BooleanValueConverter();
	private static final ValueMetadata<Double> floatingPointConverter = new FloatingPointValueConverter();
	private static final ValueMetadata<String> stringConverter = new StringValueConverter();
	private static final ValueMetadata<String> literalConverter = new LiteralValueConverter();
	
	public static final VariableHandler<Long> LARGE_INSERT_CUTOFF =
			new VariableHandler<Long>(LARGE_INSERT_THRESHOLD,
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL), 
					InvokeParser.defaultLargeInsertThreshold,
					true);
	public static final VariableHandler<Long> CACHED_PLAN_LITERALS_MAX =
			new VariableHandler<Long>(MAX_CACHED_LITERALS_NAME,
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					100L,
					true);
	public static final VariableHandler<Boolean> SLOW_QUERY_LOG =
			new VariableHandler<Boolean>(SLOW_QUERY_LOG_NAME,
					booleanConverter,
					EnumSet.of(VariableScope.GLOBAL),
					Boolean.FALSE,
					false);
	public static final VariableHandler<Long> LONG_PLAN_STEP_TIME =
			new VariableHandler<Long>(LONG_PLAN_STEP_TIME_NAME,
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					0L,
					true);
	public static final VariableHandler<Double> LONG_QUERY_TIME =
			new VariableHandler<Double>(LONG_QUERY_TIME_NAME,
					floatingPointConverter,
					EnumSet.of(VariableScope.GLOBAL),
					new Double(10.0),
					false);
	public static final VariableHandler<Boolean> EMULATE_MYSQL_LIMIT =
			new VariableHandler<Boolean>(LIMIT_ORDERBY_EMULATION,
					booleanConverter,
					EnumSet.of(VariableScope.GLOBAL),
					Boolean.FALSE,
					true);
	public static final VariableHandler<Boolean> BALANCE_PERSISTENT_GROUPS =
			new VariableHandler<Boolean>(BALANCE_PERSISTENT_GROUPS_NAME,
					booleanConverter,
					EnumSet.of(VariableScope.GLOBAL),
					Boolean.FALSE,
					true);
	public static final VariableHandler<String> BALANCE_PERSISTENT_GROUPS_PREFIX =
			new VariableHandler<String>(BALANCE_PERSISTENT_GROUPS_PREFIX_NAME,
					stringConverter,
					EnumSet.of(VariableScope.GLOBAL),
					"",
					true);
	public static final VariableHandler<String> DEFAULT_DYNAMIC_POLICY =
			new VariableHandler<String>(DEFAULT_DYNAMIC_POLICY_NAME,
					stringConverter,
					EnumSet.of(VariableScope.GLOBAL),
					OnPremiseSiteProvider.DEFAULT_POLICY_NAME,
					true);
	public static final VariableHandler<Boolean> STEPWISE_STATISTICS =
			new VariableHandler<Boolean>(STEPWISE_STATISTICS_NAME,
					booleanConverter,
					EnumSet.of(VariableScope.GLOBAL),
					Boolean.FALSE,
					true);
	public static final VariableHandler<Boolean> COST_BASED_PLANNING =
			new VariableHandler<Boolean>(CARDINALITY_COSTING_NAME,
					booleanConverter,
					EnumSet.of(VariableScope.GLOBAL),
					Boolean.TRUE,
					true);
	public static final VariableHandler<TemplateMode> TEMPLATE_MODE =
			new VariableHandler<TemplateMode>(TEMPLATE_MODE_NAME,
					new EnumValueConverter<TemplateMode>(TemplateMode.values()),
					EnumSet.of(VariableScope.GLOBAL),
					TemplateMode.REQUIRED,
					true);
	public static final VariableHandler<Boolean> SHOW_METADATA_EXTENSIONS =
			new VariableHandler<Boolean>(SHOW_METADATA_EXTENSIONS_NAME,
					booleanConverter,
					EnumSet.of(VariableScope.SESSION),
					Boolean.FALSE,
					true);
	public static final VariableHandler<Boolean> OMIT_DIST_COMMENTS =
			new VariableHandler<Boolean>(OMIT_DIST_COMMENTS_NAME,
					booleanConverter,
					EnumSet.of(VariableScope.SESSION),
					Boolean.FALSE,
					true);
	public static final VariableHandler<Long> SELECT_LIMIT =
			new VariableHandler<Long>(SQL_SELECT_LIMIT_NAME,
					new IntegralValueConverter() {
				@Override
				public Long convertToInternal(String in) throws PEException {
					if ("DEFAULT".equalsIgnoreCase(in))
						return null;
					return super.convertToInternal(in);
				}
				@Override
				public String convertToExternal(Long in) {
					if (in == null) return "DEFAULT";
					return Long.toString(in);
				}

			},
			EnumSet.of(VariableScope.SESSION),
			null,
			false);
	public static final VariableHandler<Boolean> FOREIGN_KEY_CHECKS =
			new VariableHandler<Boolean>(FOREIGN_KEY_CHECKS_NAME,
					booleanConverter,
					EnumSet.of(VariableScope.SESSION, VariableScope.GLOBAL),
					Boolean.TRUE,
					false);
	public static final VariableHandler<EngineTag> STORAGE_ENGINE =
			new VariableHandler<EngineTag>(STORAGE_ENGINE_NAME,
					new EnumValueConverter<EngineTag>(EngineTag.values()),
					EnumSet.of(VariableScope.SESSION, VariableScope.GLOBAL),
					EngineTag.INNODB,
					false);
	public static final VariableHandler<Long> REPL_INSERT_ID =
			new VariableHandler<Long>(REPL_SLAVE_INSERT_ID,
					integralConverter,
					EnumSet.of(VariableScope.SESSION),
					new Long(0),
					true);
	public static final VariableHandler<Long> REPL_TIMESTAMP =
			new VariableHandler<Long>(REPL_SLAVE_TIMESTAMP,
					integralConverter,
					EnumSet.of(VariableScope.SESSION),
					new Long(0),
					true);
	public static final VariableHandler<SQLMode> SQL_MODE =
			new VariableHandler<SQLMode>("sql_mode",
					new ValueMetadata<SQLMode>() {

						@Override
						public SQLMode convertToInternal(String in)
								throws PEException {
							return new SQLMode(in);
						}

						@Override
						public String convertToExternal(SQLMode in) {
							return in.toString();
						}
				
					},
					EnumSet.of(VariableScope.SESSION, VariableScope.GLOBAL),
					new SQLMode("NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"),
					false);
	public static final VariableHandler<String> COLLATION_CONNECTION =
			new VariableHandler<String>(COLLATION_CONNECTION_NAME,
					literalConverter,
					EnumSet.of(VariableScope.SESSION, VariableScope.GLOBAL),
					"utf8_general_ci",
					false);
	public static final VariableHandler<String> CHARACTER_SET_CLIENT =
			new VariableHandler<String>(CHARACTER_SET_CLIENT_NAME,
					literalConverter,
					EnumSet.of(VariableScope.SESSION, VariableScope.GLOBAL),
					"utf8",
					false);
	public static final VariableHandler<Long> CACHE_LIMIT =
			new VariableHandler<Long>("cache_limit",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					500L,
					true);
	public static final VariableHandler<Long> SCOPE_CACHE_LIMIT =
			new VariableHandler<Long>("scope_cache_limit",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					10000L,
					true);
	public static final VariableHandler<Long> TENANT_CACHE_LIMIT =
			new VariableHandler<Long>("tenant_cache_limit",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					500L,
					true);
	public static final VariableHandler<Long> TABLE_CACHE_LIMIT =
			new VariableHandler<Long>("table_cache_limit",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					400L,
					true);
	public static final VariableHandler<Long> PLAN_CACHE_LIMIT =
			new VariableHandler<Long>("plan_cache_limit",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					400L,
					true);
	public static final VariableHandler<Long> RAW_PLAN_CACHE_LIMIT =
			new VariableHandler<Long>("raw_plan_cache_limit",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					100L,
					true);
	public static final VariableHandler<Long> TEMPLATE_CACHE_LIMIT =
			new VariableHandler<Long>("template_cache_limit",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					100L,
					true);
	public static final VariableHandler<Long> MAX_PREPARED_STMT_COUNT =
			new VariableHandler<Long>("max_prepared_stmt_count",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					16382L,
					false);
	public static final VariableHandler<Long> REDIST_MAX_COLUMNS =
			new VariableHandler<Long>("redist_max_columns",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					1000L,
					true);
	public static final VariableHandler<Long> REDIST_MAX_SIZE =
			new VariableHandler<Long>("redist_max_size",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL),
					16000000L,
					true);
	public static final VariableHandler<Long> WAIT_TIMEOUT =
			new VariableHandler<Long>("wait_timeout",
					integralConverter,
					EnumSet.of(VariableScope.GLOBAL, VariableScope.SESSION),
					28800L,
					false);
	public static final VariableHandler<Boolean> AUTOCOMMIT =
			new VariableHandler<Boolean>("autocommit",
					booleanConverter,
					EnumSet.of(VariableScope.GLOBAL, VariableScope.SESSION),
					Boolean.TRUE,
					false) {
		
		@Override
		public void onSessionValueChange(VariableStoreSource store, Boolean newValue) throws PEException {
			((SSConnection)store).setAutoCommitMode(newValue.booleanValue());
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
					EnumSet.of(VariableScope.GLOBAL, VariableScope.SESSION),
					"+00:00",
					false);
	public static final VariableHandler<String> PERSISTENT_GROUP =
			new VariableHandler<String>("persistent_group",
					stringConverter,
					EnumSet.of(VariableScope.GLOBAL, VariableScope.SESSION),
					PEConstants.DEFAULT_GROUP_NAME,
					true);
					
		
	@SuppressWarnings("rawtypes")
	private static VariableHandler[] namedHandlers = new VariableHandler[] {
		LARGE_INSERT_CUTOFF,
		CACHED_PLAN_LITERALS_MAX,
		SLOW_QUERY_LOG,
		LONG_PLAN_STEP_TIME,
		LONG_QUERY_TIME,
		EMULATE_MYSQL_LIMIT,
		BALANCE_PERSISTENT_GROUPS,
		BALANCE_PERSISTENT_GROUPS_PREFIX,
		DEFAULT_DYNAMIC_POLICY,
		STEPWISE_STATISTICS,
		COST_BASED_PLANNING,
		TEMPLATE_MODE,
		SHOW_METADATA_EXTENSIONS,
		OMIT_DIST_COMMENTS,
		SELECT_LIMIT,
		FOREIGN_KEY_CHECKS,
		STORAGE_ENGINE,
		REPL_INSERT_ID,
		REPL_TIMESTAMP,
		SQL_MODE,
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
		AUTOCOMMIT,
		TIME_ZONE,
		PERSISTENT_GROUP
	};
	
	@SuppressWarnings("rawtypes")
	private static final VariableHandler[] unnamedHandlers = new VariableHandler[] {
		new VariableHandler<Long>("group_concat_max_len",
				integralConverter,
				EnumSet.of(VariableScope.SESSION, VariableScope.GLOBAL),
				1024L,
				false),
	};
	
	private static final Map<String,VariableHandler<?>> allHandlers = buildAllHandlers();
		
	private static Map<String,VariableHandler<?>> buildAllHandlers() {
		HashMap<String,VariableHandler<?>> out = new HashMap<String,VariableHandler<?>>();
		for(VariableHandler<?> vh : namedHandlers) 
			out.put(vh.getName(), vh);
		for(VariableHandler<?> vh : unnamedHandlers)
			out.put(vh.getName(), vh);
		
		return out;
	}
	
	public static List<VariableHandler<?>> getSessionHandlers() {
		return Functional.select(allHandlers.values(), VariableHandler.isSessionPredicate);
	}
	
	public static List<VariableHandler<?>> getGlobalHandlers() {
		return Functional.select(allHandlers.values(), VariableHandler.isGlobalPredicate);
	}
	
	public static VariableHandler<?> lookup(String name, boolean except) throws PEException {
		VariableHandler<?> vh = allHandlers.get(name);
		if (vh == null && except)
			throw new PEException(String.format("No such variable: '%s'", name));
		return vh;
		
	}
	
	
}

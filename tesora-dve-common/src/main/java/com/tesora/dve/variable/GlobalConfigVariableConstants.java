// OS_STATUS: public
package com.tesora.dve.variable;

public interface GlobalConfigVariableConstants {
	public static final String STATISTICS_INTERVAL = "statistics_interval";

	public static final String ADAPTIVE_CLEANUP_INTERVAL = "adaptive_cleanup_interval";

	public static final String PERF_TIMING_LOG_LEVEL = "perf_timing_log_level";
	public static final String SQL_LOGGING = "sql_logging";
	
	public static final String LARGE_INSERT_THRESHOLD = SchemaVariableConstants.LARGE_INSERT_THRESHOLD;
	public static final String SLOW_QUERY_LOG = SchemaVariableConstants.SLOW_QUERY_LOG_NAME;
	public static final String LONG_PLAN_STEP_TIME = SchemaVariableConstants.LONG_PLAN_STEP_TIME_NAME;
	public static final String LONG_QUERY_TIME = SchemaVariableConstants.LONG_QUERY_TIME_NAME;
	public static final String STEPWISE_STATISTICS = SchemaVariableConstants.STEPWISE_STATISTICS_NAME;
	public static final String TEMPLATE_MODE = SchemaVariableConstants.TEMPLATE_MODE_NAME;

	public static final String BALANCE_PERSISTENT_GROUPS = SchemaVariableConstants.BALANCE_PERSISTENT_GROUPS_NAME;
	public static final String BALANCE_PERSISTENT_GROUPS_PREFIX = SchemaVariableConstants.BALANCE_PERSISTENT_GROUPS_PREFIX_NAME;
	
	public static final String GROUP_SERVICE = "group_service";
	
	public static final String MAX_CACHED_PLAN_LITERALS = "max_cached_plan_literals";
	public static final String PE_MYSQL_EMULATE_LIMIT = SchemaVariableConstants.LIMIT_ORDERBY_EMULATION;
	public static final String INNODB_LOCK_WAIT_TIMEOUT = "innodb_lock_wait_timeout";
	public static final String WAIT_TIMEOUT = "wait_timeout";
	public static final String REDIST_MAX_COLUMNS = "redist_max_columns";
	public static final String REDIST_MAX_SIZE = "redist_max_size";
	public static final String GROUP_CONCAT_MAX_LEN = "group_concat_max_len";
	
}

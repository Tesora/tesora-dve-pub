// OS_STATUS: public
package com.tesora.dve.variable;

public interface SchemaVariableConstants {
	
	// pe specific session variables
	public static final String SHOW_METADATA_EXTENSIONS_NAME = "dve_metadata_extensions";
	public static final String REPL_SLAVE_INSERT_ID = "dve_repl_slave_insert_id";
	public static final String REPL_SLAVE_TIMESTAMP = "dve_repl_slave_timestamp";
	public static final String OMIT_DIST_COMMENTS_NAME = "dve_omit_comments";
	
	// regular session variables
	public static final String SQL_SELECT_LIMIT_NAME = "sql_select_limit";
	public static final String SQL_AUTO_IS_NULL = "sql_auto_is_null";
	public static final String FOREIGN_KEY_CHECKS_NAME = "foreign_key_checks";
	public static final String STORAGE_ENGINE_NAME = "storage_engine";
	public static final String CHARACTER_SET_CLIENT_NAME = "character_set_client";
	public static final String COLLATION_CONNECTION_NAME = "collation_connection";
	
	// relating to perflogging - we manage in schema mostly for the fast variable access
	public static final String SLOW_QUERY_LOG_NAME = "slow_query_log";
	public static final String LONG_PLAN_STEP_TIME_NAME = "long_plan_step_time";
	public static final String LONG_QUERY_TIME_NAME = "long_query_time";
	
	// planning related constants
	public static final String LARGE_INSERT_THRESHOLD = "large_insert_threshold";
	public static final String LIMIT_ORDERBY_EMULATION = "dve_mysql_emulate_limit";
	public static final String MAX_CACHED_LITERALS_NAME = "max_cached_plan_literals";
	public static final String STEPWISE_STATISTICS_NAME = "stepwise_statistics";
	public static final String CARDINALITY_COSTING_NAME = "cost_based_planning";

	// mt related
	public static final String TABLE_GARBAGE_COLLECTOR_INTERVAL_NAME = "adaptive_cleanup_interval";
	
	// dynamic persistent group related
	public static final String BALANCE_PERSISTENT_GROUPS_NAME = "balance_persistent_groups";
	public static final String BALANCE_PERSISTENT_GROUPS_PREFIX_NAME = "balance_persistent_groups_prefix";
	
	// dynamic policy
	public static final String DEFAULT_DYNAMIC_POLICY_NAME = "default_dynamic_policy";
	
	// template related
	public static final String TEMPLATE_MODE_NAME = "template_mode";
}

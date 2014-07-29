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

public interface VariableConstants {

	// pe specific session variables
	public static final String SHOW_METADATA_EXTENSIONS_NAME = "dve_metadata_extensions";
	public static final String REPL_SLAVE_INSERT_ID_NAME = "dve_repl_slave_insert_id";
	public static final String REPL_SLAVE_TIMESTAMP_NAME = "dve_repl_slave_timestamp";
	public static final String OMIT_DIST_COMMENTS_NAME = "dve_omit_comments";
	
	// regular session variables
	public static final String SQL_SELECT_LIMIT_NAME = "sql_select_limit";
	public static final String SQL_AUTO_IS_NULL_NAME = "sql_auto_is_null";
	public static final String FOREIGN_KEY_CHECKS_NAME = "foreign_key_checks";
	public static final String STORAGE_ENGINE_NAME = "storage_engine";
	public static final String CHARACTER_SET_CLIENT_NAME = "character_set_client";
	public static final String COLLATION_CONNECTION_NAME = "collation_connection";
	
	// relating to perflogging - we manage in schema mostly for the fast variable access
	public static final String SLOW_QUERY_LOG_NAME = "slow_query_log";
	public static final String LONG_PLAN_STEP_TIME_NAME = "long_plan_step_time";
	public static final String LONG_QUERY_TIME_NAME = "long_query_time";
	
	// planning related constants
	public static final String LARGE_INSERT_THRESHOLD_NAME = "large_insert_threshold";
	public static final String LIMIT_ORDERBY_EMULATION_NAME = "dve_mysql_emulate_limit";
	public static final String MAX_CACHED_LITERALS_NAME = "max_cached_plan_literals";
	public static final String STEPWISE_STATISTICS_NAME = "stepwise_statistics";
	public static final String CARDINALITY_COSTING_NAME = "cost_based_planning";

	// mt related
	public static final String TABLE_GARBAGE_COLLECTOR_INTERVAL_NAME = "adaptive_cleanup_interval";
	
	// dynamic persistent group related
	public static final String BALANCE_PERSISTENT_GROUPS_NAME = "balance_persistent_groups";
	public static final String BALANCE_PERSISTENT_GROUPS_PREFIX_NAME = "balance_persistent_groups_prefix";
	
	// dynamic policy
	public static final String DYNAMIC_POLICY_NAME = "dynamic_policy";
	
	// template related
	public static final String TEMPLATE_MODE_NAME = "template_mode";

	public static final String STATISTICS_INTERVAL_NAME = "statistics_interval";

	public static final String ADAPTIVE_CLEANUP_INTERVAL_NAME = "adaptive_cleanup_interval";

	public static final String PERF_TIMING_LOG_LEVEL_NAME = "perf_timing_log_level";
	public static final String SQL_LOGGING_NAME = "sql_logging";
	
	public static final String GROUP_SERVICE_NAME = "group_service";
	
	public static final String MAX_CACHED_PLAN_LITERALS_NAME = "max_cached_plan_literals";
	public static final String INNODB_LOCK_WAIT_TIMEOUT_NAME = "innodb_lock_wait_timeout";
	public static final String WAIT_TIMEOUT_NAME = "wait_timeout";
	public static final String REDIST_MAX_COLUMNS_NAME = "redist_max_columns";
	public static final String REDIST_MAX_SIZE_NAME = "redist_max_size";
	public static final String GROUP_CONCAT_MAX_LEN_NAME = "group_concat_max_len";
	
	public static final String AUTO_INCREMENT_MIN_BLOCK_SIZE = "auto_increment_min_block_size";
	public static final String AUTO_INCREMENT_MAX_BLOCK_SIZE = "auto_increment_max_block_size";
	public static final String AUTO_INCREMENT_PREFETCH_THRESHOLD = "auto_increment_prefetch_threshold";


}

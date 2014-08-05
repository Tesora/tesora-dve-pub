package com.tesora.dve.variable.status;

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

import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.common.catalog.StorageGroup.GroupScale;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.PlannerStatisticType;
import com.tesora.dve.sql.parser.SqlStatistics;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.variables.VariableManager;

public class StatusVariables {

	private static final StatusVariableHandler[] statusVariables = new StatusVariableHandler[] {
		new StatusVariableHandler("Queries") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(SqlStatistics.getTotalValue());
			}

			@Override
			protected void resetValueInternal() throws Throwable {
				SqlStatistics.resetAllValues();
			}
			
		},
		new StatusVariableHandler("Questions") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(SqlStatistics.getTotalValue());
			}

			@Override
			protected void resetValueInternal() throws Throwable {
				SqlStatistics.resetAllValues();
			}
			
		},
		new SqlStatusVariableHandler("Com_begin",StatementType.BEGIN),
		new SqlStatusVariableHandler("Com_commit",StatementType.COMMIT),
		new SqlStatusVariableHandler("Com_create_db",StatementType.CREATE_DB),
		new SqlStatusVariableHandler("Com_create_table",StatementType.CREATE_TABLE),
		new SqlStatusVariableHandler("Com_create_index",StatementType.CREATE_INDEX),
		new SqlStatusVariableHandler("Com_create_user",StatementType.CREATE_USER),
		new SqlStatusVariableHandler("Com_delete",StatementType.DELETE),
		new SqlStatusVariableHandler("Com_drop_db",StatementType.DROP_DB),
		new SqlStatusVariableHandler("Com_drop_table",StatementType.DROP_TABLE),
		new SqlStatusVariableHandler("Com_drop_index",StatementType.DROP_INDEX),
		new SqlStatusVariableHandler("Com_grant",StatementType.GRANT),
		new SqlStatusVariableHandler("Com_insert",StatementType.INSERT),
		new SqlStatusVariableHandler("Com_insert_select",StatementType.INSERT_INTO_SELECT),
		new SqlStatusVariableHandler("Com_lock_tables",StatementType.LOCK_TABLES),
		new SqlStatusVariableHandler("Com_rollback",StatementType.ROLLBACK),
		new SqlStatusVariableHandler("Com_show_processlist",StatementType.SHOW_PROCESSLIST),
		new SqlStatusVariableHandler("Com_show_grants",StatementType.SHOW_GRANTS),
		new SqlStatusVariableHandler("Com_show_master_status",StatementType.SHOW_MASTER_STATUS),
		new SqlStatusVariableHandler("Com_show_slave_status",StatementType.SHOW_SLAVE_STATUS),
		new SqlStatusVariableHandler("Com_show_plugins",StatementType.SHOW_PLUGINS),
		new SqlStatusVariableHandler("Com_truncate",StatementType.TRUNCATE),
		new SqlStatusVariableHandler("Com_unlock_tables",StatementType.UNLOCK_TABLES),
		new SqlStatusVariableHandler("Com_update",StatementType.UPDATE),
		new SqlStatusVariableHandler("Com_stmt_close",StatementType.CLOSE_PREPARE),
		new SqlStatusVariableHandler("Com_stmt_execute",StatementType.EXEC_PREPARE),
		new SqlStatusVariableHandler("Com_stmt_prepare",StatementType.PREPARE),
		
		new StatusVariableHandler("Dve_memory_max") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(Runtime.getRuntime().maxMemory());
			}
			
		},
		new StatusVariableHandler("Dve_memory_total") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(Runtime.getRuntime().totalMemory());
			}
			
		},
		new StatusVariableHandler("Dve_memory_free") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(Runtime.getRuntime().freeMemory());
			}
			
		},
		new StatusVariableHandler("Dve_memory_used") {

			@Override
			protected String getValueInternal() throws Throwable {
				Runtime rt = Runtime.getRuntime();
				return Long.toString(rt.totalMemory() - rt.freeMemory());
			}
			
		},
		new StatusVariableHandler("Dve_threads_active") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(Thread.activeCount());
			}
			
		},
		new RedistStatusVariableHandler("Dve_redist_aggregation",GroupScale.AGGREGATE),
		new RedistStatusVariableHandler("Dve_redist_small",GroupScale.SMALL),
		new RedistStatusVariableHandler("Dve_redist_medium",GroupScale.MEDIUM),
		new RedistStatusVariableHandler("Dve_redist_large",GroupScale.LARGE),
		new PlannerStatisticsVariableHandler("Dve_plan_cache_uncacheable_string",PlannerStatisticType.UNCACHEABLE_CANDIDATE_PARSE),
		new PlannerStatisticsVariableHandler("Dve_plan_cache_invalid_key",PlannerStatisticType.UNCACHEABLE_INVALID_CANDIDATE),
		new PlannerStatisticsVariableHandler("Dve_plan_cache_uncacheable_maxliterals",PlannerStatisticType.UNCACHEABLE_EXCEEDED_LITERAL_LIMIT),
		new PlannerStatisticsVariableHandler("Dve_plan_cache_uncacheable_dyn_funct",PlannerStatisticType.UNCACHEABLE_DYNAMIC_FUNCTION),
		new PlannerStatisticsVariableHandler("Dve_plan_cache_uncacheable",PlannerStatisticType.UNCACHEABLE_PLAN),
		
		new CacheStatusVariableHandler(CacheSegment.UNCATEGORIZED,CacheStatusVariableHandler.sizeHandler),
		new CacheStatusVariableHandler(CacheSegment.UNCATEGORIZED,CacheStatusVariableHandler.utilizationHandler),
		new CacheStatusVariableHandler(CacheSegment.UNCATEGORIZED,CacheStatusVariableHandler.evictionHandler),
		new CacheStatusVariableHandler(CacheSegment.UNCATEGORIZED,CacheStatusVariableHandler.loadTimeHandler),
		new CacheStatusVariableHandler(CacheSegment.UNCATEGORIZED,CacheStatusVariableHandler.hitRateHandler),
		new CacheStatusVariableHandler(CacheSegment.UNCATEGORIZED,CacheStatusVariableHandler.missRateHandler),

		new CacheStatusVariableHandler(CacheSegment.PLAN,CacheStatusVariableHandler.sizeHandler),
		new CacheStatusVariableHandler(CacheSegment.PLAN,CacheStatusVariableHandler.utilizationHandler),
		new CacheStatusVariableHandler(CacheSegment.PLAN,CacheStatusVariableHandler.evictionHandler),
		new CacheStatusVariableHandler(CacheSegment.PLAN,CacheStatusVariableHandler.hitRateHandler),
		new CacheStatusVariableHandler(CacheSegment.PLAN,CacheStatusVariableHandler.missRateHandler),

		new CacheStatusVariableHandler(CacheSegment.SCOPE,CacheStatusVariableHandler.sizeHandler),
		new CacheStatusVariableHandler(CacheSegment.SCOPE,CacheStatusVariableHandler.utilizationHandler),
		new CacheStatusVariableHandler(CacheSegment.SCOPE,CacheStatusVariableHandler.evictionHandler),
		new CacheStatusVariableHandler(CacheSegment.SCOPE,CacheStatusVariableHandler.loadTimeHandler),
		new CacheStatusVariableHandler(CacheSegment.SCOPE,CacheStatusVariableHandler.hitRateHandler),
		new CacheStatusVariableHandler(CacheSegment.SCOPE,CacheStatusVariableHandler.missRateHandler),

		new CacheStatusVariableHandler(CacheSegment.TABLE,CacheStatusVariableHandler.sizeHandler),
		new CacheStatusVariableHandler(CacheSegment.TABLE,CacheStatusVariableHandler.utilizationHandler),
		new CacheStatusVariableHandler(CacheSegment.TABLE,CacheStatusVariableHandler.evictionHandler),
		new CacheStatusVariableHandler(CacheSegment.TABLE,CacheStatusVariableHandler.loadTimeHandler),
		new CacheStatusVariableHandler(CacheSegment.TABLE,CacheStatusVariableHandler.hitRateHandler),
		new CacheStatusVariableHandler(CacheSegment.TABLE,CacheStatusVariableHandler.missRateHandler),

		new CacheStatusVariableHandler(CacheSegment.TENANT,CacheStatusVariableHandler.sizeHandler),
		new CacheStatusVariableHandler(CacheSegment.TENANT,CacheStatusVariableHandler.utilizationHandler),
		new CacheStatusVariableHandler(CacheSegment.TENANT,CacheStatusVariableHandler.evictionHandler),
		new CacheStatusVariableHandler(CacheSegment.TENANT,CacheStatusVariableHandler.loadTimeHandler),
		new CacheStatusVariableHandler(CacheSegment.TENANT,CacheStatusVariableHandler.hitRateHandler),
		new CacheStatusVariableHandler(CacheSegment.TENANT,CacheStatusVariableHandler.missRateHandler),

		new CacheStatusVariableHandler(CacheSegment.RAWPLAN,CacheStatusVariableHandler.sizeHandler),
		new CacheStatusVariableHandler(CacheSegment.RAWPLAN,CacheStatusVariableHandler.utilizationHandler),
		new CacheStatusVariableHandler(CacheSegment.RAWPLAN,CacheStatusVariableHandler.evictionHandler),
		new CacheStatusVariableHandler(CacheSegment.RAWPLAN,CacheStatusVariableHandler.loadTimeHandler),
		new CacheStatusVariableHandler(CacheSegment.RAWPLAN,CacheStatusVariableHandler.hitRateHandler),
		new CacheStatusVariableHandler(CacheSegment.RAWPLAN,CacheStatusVariableHandler.missRateHandler),

		new CacheStatusVariableHandler(CacheSegment.TEMPLATE,CacheStatusVariableHandler.sizeHandler),
		new CacheStatusVariableHandler(CacheSegment.TEMPLATE,CacheStatusVariableHandler.utilizationHandler),
		new CacheStatusVariableHandler(CacheSegment.TEMPLATE,CacheStatusVariableHandler.evictionHandler),
		new CacheStatusVariableHandler(CacheSegment.TEMPLATE,CacheStatusVariableHandler.loadTimeHandler),
		new CacheStatusVariableHandler(CacheSegment.TEMPLATE,CacheStatusVariableHandler.hitRateHandler),
		new CacheStatusVariableHandler(CacheSegment.TEMPLATE,CacheStatusVariableHandler.missRateHandler),

		new CacheStatusVariableHandler(CacheSegment.PREPARED,CacheStatusVariableHandler.sizeHandler),
		new CacheStatusVariableHandler(CacheSegment.PREPARED,CacheStatusVariableHandler.utilizationHandler),
		new CacheStatusVariableHandler(CacheSegment.PREPARED,CacheStatusVariableHandler.evictionHandler),
		new CacheStatusVariableHandler(CacheSegment.PREPARED,CacheStatusVariableHandler.loadTimeHandler),
		new CacheStatusVariableHandler(CacheSegment.PREPARED,CacheStatusVariableHandler.hitRateHandler),
		new CacheStatusVariableHandler(CacheSegment.PREPARED,CacheStatusVariableHandler.missRateHandler),

		new StatusVariableHandler("Uptime") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(Singletons.require(HostService.class).getServerUptime());
			}
			
			@Override
			protected void resetValueInternal() throws Throwable {
				Singletons.require(HostService.class).resetServerUptime();
			}
			
		},
		
		new StatusVariableHandler("Threads_connected") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Integer.toString(PerHostConnectionManager.INSTANCE.getConnectionCount());
			}
		},
		
		new StatusVariableHandler("Connections") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(PerHostConnectionManager.INSTANCE.getTotalConnections());
			}

			@Override
			protected void resetValueInternal() throws Throwable {
				PerHostConnectionManager.INSTANCE.resetTotalConnections();
			}
			
		},
		
		new StatusVariableHandler("Connections_max_concurrent") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(PerHostConnectionManager.INSTANCE.getMaxConcurrentConnections());
			}

			@Override
			protected void resetValueInternal() throws Throwable {
				PerHostConnectionManager.INSTANCE.resetMaxConcurrentConnections();
			}
		},
		
		new StatusVariableHandler("Aborted_connects") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(PerHostConnectionManager.INSTANCE.getTotalConnectionFailures());
			}

			@Override
			protected void resetValueInternal() throws Throwable {
				PerHostConnectionManager.INSTANCE.resetTotalConnectionFailures();
			}

		},
		
		new StatusVariableHandler("Aborted_clients") {

			@Override
			protected String getValueInternal() throws Throwable {
				return Long.toString(PerHostConnectionManager.INSTANCE.getTotalClientFailures());
			}

			@Override
			protected void resetValueInternal() throws Throwable {
				PerHostConnectionManager.INSTANCE.resetTotalClientFailures();
			}

			
		},
	};

	
	private static final Map<String,StatusVariableHandler> statusVariableMap = buildStatusVariableMap();
	
	public static StatusVariableHandler[] getStatusVariables() {
		return statusVariables;
	}
	
	public static StatusVariableHandler lookup(String name, boolean except) throws PENotFoundException {
		StatusVariableHandler handler = statusVariableMap.get(VariableManager.normalize(name));
		if (handler == null && except)
			throw new PENotFoundException("No such status variable: '" + name + "'");
		return handler;
	}
	
	private static Map<String,StatusVariableHandler> buildStatusVariableMap() {
		HashMap<String,StatusVariableHandler> out = new HashMap<String,StatusVariableHandler>();
		for(StatusVariableHandler svh : statusVariables) {
			out.put(VariableManager.normalize(svh.getName()), svh);
		}
		return out;
	}
	
	/*
	 * Variables to add at some point
	 * 
	 * 		<Variable name="Binlog_cache_disk_use" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Binlog_cache_use" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Binlog_stmt_cache_disk_use" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Binlog_stmt_cache_use" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Bytes_received" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Bytes_sent" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_admin_commands" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_assign_to_keycache" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_alter_db" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_alter_db_upgrade" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_alter_event" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_alter_function" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_alter_procedure" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_alter_server" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_alter_table" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_alter_tablespace" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_analyze" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_binlog" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_call_procedure" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_change_db" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_change_master" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_check" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_checksum" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_create_event" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_create_function" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_create_procedure" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_create_server" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_create_trigger" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_create_udf" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_create_view" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_dealloc_sql" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_delete_multi" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_do" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_drop_event" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_drop_function" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_drop_procedure" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_drop_server" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_drop_trigger" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_drop_user" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_drop_view" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_empty_query" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_execute_sql" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_flush" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_ha_close" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_ha_open" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_ha_read" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_help" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_install_plugin" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_kill" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_load" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_optimize" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_preload_keys" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_prepare_sql" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_purge" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_purge_before_date" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_release_savepoint" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_rename_table" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_rename_user" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_repair" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_replace" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_replace_select" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_reset" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_resignal" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_revoke" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_revoke_all" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_rollback_to_savepoint" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_savepoint" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_set_option" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_signal" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_authors" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_binlog_events" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_binlogs" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_charsets" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_collations" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_contributors" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_create_db" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_create_event" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_create_func" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_create_proc" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_create_table" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_create_trigger" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_databases" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_engine_logs" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_engine_mutex" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_engine_status" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_events" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_errors" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_fields" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_function_status" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_keys" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_open_tables" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_privileges" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_procedure_status" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_profile" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_profiles" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_relaylog_events" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_slave_hosts" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_status" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_storage_engines" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_table_status" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_tables" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_triggers" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_variables" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_show_warnings" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_slave_start" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_slave_stop" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_stmt_fetch" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_stmt_reprepare" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_stmt_reset" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_stmt_send_long_data" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_uninstall_plugin" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_update_multi" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_xa_commit" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_xa_end" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_xa_prepare" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_xa_recover" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_xa_rollback" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Com_xa_start" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Compression" defaultValue="OFF" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Connections" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Created_tmp_disk_tables" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Created_tmp_files" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Created_tmp_tables" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Delayed_errors" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Delayed_insert_threads" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Delayed_writes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Flush_commands" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_commit" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_delete" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_discover" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_prepare" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_read_first" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_read_key" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_read_last" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_read_next" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_read_prev" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_read_rnd" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_read_rnd_next" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_rollback" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_savepoint" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_savepoint_rollback" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_update" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Handler_write" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_pages_data" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_pages_dirty" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_pages_flushed" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_pages_free" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_pages_misc" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_pages_total" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_read_ahead_rnd" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_read_ahead" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_read_ahead_evicted" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_read_requests" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_reads" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_wait_free" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_buffer_pool_write_requests" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_data_fsyncs" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_data_pending_fsyncs" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_data_pending_reads" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_data_pending_writes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_data_read" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_data_reads" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_data_writes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_data_written" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_dblwr_pages_written" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_dblwr_writes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_have_atomic_builtins" defaultValue="ON" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_log_waits" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_log_write_requests" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_log_writes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_os_log_fsyncs" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_os_log_pending_fsyncs" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_os_log_pending_writes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_os_log_written" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_page_size" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_pages_created" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_pages_read" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_pages_written" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_row_lock_current_waits" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_row_lock_time" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_row_lock_time_avg" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_row_lock_time_max" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_row_lock_waits" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_rows_deleted" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_rows_inserted" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_rows_read" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_rows_updated" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Innodb_truncated_status_writes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Key_blocks_not_flushed" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Key_blocks_unused" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Key_blocks_used" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Key_read_requests" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Key_reads" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Key_write_requests" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Key_writes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Last_query_cost" defaultValue="0.000000" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Max_used_connections" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Not_flushed_delayed_rows" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Open_files" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Open_streams" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Open_table_definitions" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Open_tables" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Opened_files" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Opened_table_definitions" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Opened_tables" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_cond_classes_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_cond_instances_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_file_classes_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_file_handles_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_file_instances_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_locker_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_mutex_classes_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_mutex_instances_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_rwlock_classes_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_rwlock_instances_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_table_handles_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_table_instances_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_thread_classes_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Performance_schema_thread_instances_lost" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Prepared_stmt_count" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Qcache_free_blocks" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Qcache_free_memory" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Qcache_hits" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Qcache_inserts" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Qcache_lowmem_prunes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Qcache_not_cached" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Qcache_queries_in_cache" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Qcache_total_blocks" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Rpl_status" defaultValue="AUTH_MASTER" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Select_full_join" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Select_full_range_join" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Select_range" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Select_range_check" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Select_scan" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Slave_heartbeat_period" defaultValue="0.000" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Slave_open_temp_tables" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Slave_received_heartbeats" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Slave_retried_transactions" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Slave_running" defaultValue="OFF" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Slow_launch_threads" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Slow_queries" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Sort_merge_passes" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Sort_range" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Sort_rows" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Sort_scan" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_accept_renegotiates" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_accepts" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_callback_cache_hits" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_cipher" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_cipher_list" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_client_connects" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_connect_renegotiates" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_ctx_verify_depth" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_ctx_verify_mode" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_default_timeout" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_finished_accepts" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_finished_connects" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_session_cache_hits" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_session_cache_misses" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_session_cache_mode" defaultValue="NONE" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_session_cache_overflows" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_session_cache_size" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_session_cache_timeouts" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_sessions_reused" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_used_session_cache_entries" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_verify_depth" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_verify_mode" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Ssl_version" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Table_locks_immediate" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Table_locks_waited" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Tc_log_max_pages_used" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Tc_log_page_size" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Tc_log_page_waits" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Threads_cached" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Threads_created" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Threads_running" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>
		<Variable name="Uptime_since_flush_status" defaultValue="0" handler="com.tesora.dve.variable.status.DefaultStatusVariableHandler"/>

	 */
}

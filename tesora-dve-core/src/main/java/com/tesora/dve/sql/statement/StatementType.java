package com.tesora.dve.sql.statement;

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

public enum StatementType {

	// dml
	SELECT,
	UPDATE,
	DELETE,
	INSERT,
	INSERT_INTO_SELECT,
	UNION,
	TRUNCATE,
	LOCK_TABLES,
	UNLOCK_TABLES,
	
	// ddl
	CREATE_DB,
	CREATE_TABLE,
	CREATE_INDEX,
	CREATE_USER,
	DROP_DB,
	DROP_TABLE,
	DROP_INDEX,
	GRANT,
	
	// txnal,
	BEGIN,
	COMMIT,
	ROLLBACK,
	
	SHOW_GRANTS,
	SHOW_PROCESSLIST,
	SHOW_MASTER_STATUS,
	SHOW_SLAVE_STATUS,
	SHOW_PLUGINS,
	
	// other stuff
	EXPLAIN,
	PREPARE,
	EXEC_PREPARE,
	CLOSE_PREPARE,
	ANALYZE_TABLE,
	CHECK_TABLE,
	OPTIMIZE_TABLE,
	
	// should be last
	UNIMPORTANT;
}

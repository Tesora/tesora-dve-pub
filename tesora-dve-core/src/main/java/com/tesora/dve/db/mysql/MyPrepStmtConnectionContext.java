// OS_STATUS: public
package com.tesora.dve.db.mysql;

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

import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import io.netty.util.AttributeKey;

import com.tesora.dve.server.connectionmanager.SSConnection;


public class MyPrepStmtConnectionContext implements MyConnectionContext {
	public static AttributeKey<MyPrepStmtConnectionContext> PSTMT_CONTEXT_KEY = new AttributeKey<MyPrepStmtConnectionContext>("PrepStmtContext");

	private final SSConnection conn;
	
	// this tracks the next prepared stmt id for this connection
	long preparedStmtId = 0;

	public MyPrepStmtConnectionContext(SSConnection sscon) {
		conn = sscon;
	}
	
	public long getPreparedStatementId() {
		return preparedStmtId;
	}
	
	public MyPreparedStatement<String> getPreparedStatement(long stmtId) {
		return conn.getPreparedStatement(Long.toString(stmtId));
	}
	
	public MyPreparedStatement<String> addPreparedStatement(MyPreparedStatement<String> pStmt) {
		long id = ++preparedStmtId;
		pStmt.setStmtId(Long.toString(id));
		conn.putPreparedStatement(Long.toString(id), pStmt);
		return pStmt;
	}
	
	public void removePreparedStatement(long stmtId) {
		conn.removePreparedStatement(Long.toString(stmtId));
	}
	
	public void removePreparedStatement(MyPreparedStatement<Long> pStmt) {
		removePreparedStatement(pStmt.getStmtId());
	}

	public void clearPreparedStatements() {
		conn.clearPreparedStatements();
	}
}

// OS_STATUS: public
package com.tesora.dve.db.mysql;

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

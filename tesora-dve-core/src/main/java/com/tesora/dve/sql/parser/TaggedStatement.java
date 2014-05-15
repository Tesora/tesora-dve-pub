// OS_STATUS: public
package com.tesora.dve.sql.parser;

public class TaggedStatement {

	protected String statement;
	protected int connectionTag;
	protected MysqlLogFileEntryKind kind;
	
	public TaggedStatement(String stmt, int conn, MysqlLogFileEntryKind sk) {
		statement = stmt;
		connectionTag = conn;
		kind = sk;
	}
	
	public TaggedStatement(String stmt) {
		this(stmt,-1,MysqlLogFileEntryKind.QUERY);
	}
	
	public String getStatement() {
		return statement;
	}
	
	public int getConnectionID() {
		return connectionTag;
	}
	
	public MysqlLogFileEntryKind getEntryKind() {
		return kind;
	}
}

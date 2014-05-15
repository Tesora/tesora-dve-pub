// OS_STATUS: public
package com.tesora.dve.sql.statement.session;


import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.SessionExecutionStep;

public class SessionStatement extends Statement {

	protected String sql;
	
	public SessionStatement() {
		this(null);
	}
	
	public SessionStatement(String adhocsql) {
		super(null);
		sql = adhocsql;
	}
		
	@Override
	public boolean isSession() {
		return true;
	}

	public boolean isAdhoc() {
		return sql != null;
	}
	
	public String getAdhocSQL() {
		return sql;
	}
	
	@Override
	public boolean isDML() {
		return false;
	}
	
	public boolean isPassthrough() {
		// for the most part this is true
		return true;
	}
	
	@Override
	public void normalize(SchemaContext sc) {
		// TODO Auto-generated method stub
	}

	@Override
	public Database<?> getDatabase(SchemaContext sc) {
		// may not actually be necessary
		return sc.getCurrentDatabase(false);
	}
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {
		if (isPassthrough()) {
			es.append(new SessionExecutionStep(getDatabase(sc), getStorageGroup(sc), getSQL(sc)));
		} else {
			unhandledStatement();
		}
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return illegalSchemaSelf(other);
	}

	@Override
	protected int selfHashCode() {
		return illegalSchemaHash();
	}
	
}

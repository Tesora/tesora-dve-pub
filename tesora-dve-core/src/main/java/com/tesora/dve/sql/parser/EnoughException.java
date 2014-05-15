// OS_STATUS: public
package com.tesora.dve.sql.parser;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;

public class EnoughException extends SchemaException {

	private static final long serialVersionUID = 1L;

	private final InsertIntoValuesStatement completed;
	
	public EnoughException(InsertIntoValuesStatement stmt) {
		super(Pass.FIRST);
		completed = stmt;
	}
	
	public InsertIntoValuesStatement getInsertStatement() {
		return completed;
	}
	
}

// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

public abstract class DropStatement extends DDLStatement {

	private boolean ifExists;
	
	public DropStatement(Boolean ifExists, boolean peOnly) {
		super(peOnly);
		this.ifExists = (ifExists == null ? false : ifExists.booleanValue());
	}
	
	public boolean isIfExists() {
		return this.ifExists;
	}
	
	
}

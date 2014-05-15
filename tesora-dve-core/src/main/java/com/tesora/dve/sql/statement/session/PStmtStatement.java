// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.sql.schema.UnqualifiedName;

public class PStmtStatement extends SessionStatement {

	protected UnqualifiedName name;
	
	public PStmtStatement(UnqualifiedName unq) {
		super();
		this.name = unq;
	}
	
	@Override
	public boolean isPassthrough() {
		return false;
	}
	
	public UnqualifiedName getName() {
		return this.name;
	}
	
}

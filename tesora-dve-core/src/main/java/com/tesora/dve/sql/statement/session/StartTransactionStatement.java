// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

public class StartTransactionStatement extends TransactionStatement {

	private final boolean consistent;
	
	public StartTransactionStatement(boolean consistentSnapshot) {
		super(Kind.START);
		consistent = consistentSnapshot;
	}
		
	@Override
	public boolean isConsistent() {
		return consistent;
	}

	
}

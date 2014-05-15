// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.server.connectionmanager.UserXid;

public class XACommitTransactionStatement extends XATransactionStatement {

	private final boolean onePhase;
	
	public XACommitTransactionStatement(UserXid xid, boolean onePhase) {
		super(TransactionStatement.Kind.COMMIT, xid);
		this.onePhase = onePhase;
	}

	public boolean isOnePhase() {
		return this.onePhase;
	}
	
}

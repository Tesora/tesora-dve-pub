// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.server.connectionmanager.UserXid;

public abstract class XATransactionStatement extends TransactionStatement {

	protected final UserXid xid;
	
	public XATransactionStatement(TransactionStatement.Kind kind,UserXid xid) {
		super(kind);
		this.xid = xid;
	}
	
	public UserXid getXAXid() {
		return xid;
	}	
	
}

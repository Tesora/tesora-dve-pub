// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.server.connectionmanager.UserXid;

public class XARollbackTransactionStatement extends XATransactionStatement {

	public XARollbackTransactionStatement(UserXid xid) {
		super(TransactionStatement.Kind.ROLLBACK, xid);
	}

}

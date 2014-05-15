// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.server.connectionmanager.UserXid;

public class XABeginTransactionStatement extends XATransactionStatement {

	public XABeginTransactionStatement(UserXid xid) {
		super(TransactionStatement.Kind.START,xid);
	}
}

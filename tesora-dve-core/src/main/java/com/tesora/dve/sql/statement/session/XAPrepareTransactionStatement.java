// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.server.connectionmanager.UserXid;

public class XAPrepareTransactionStatement extends XATransactionStatement {

	public XAPrepareTransactionStatement(UserXid xid) {
		super(TransactionStatement.Kind.PREPARE, xid);
	}

}

// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.server.connectionmanager.UserXid;

public class XAEndTransactionStatement extends XATransactionStatement {

	public XAEndTransactionStatement(UserXid xid) {
		super(TransactionStatement.Kind.END, xid);
	}

}

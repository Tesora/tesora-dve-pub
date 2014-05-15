// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class CommitTransactionRequestExecutor implements AgentExecutor<SSConnection> {

	@Override
	public ResponseMessage execute(SSConnection connMgr, Object message) throws Throwable {
		connMgr.userCommitTransaction();
		return new GenericResponse().success();
	}

}

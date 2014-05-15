// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import com.tesora.dve.comms.client.messages.GetTransactionStatusResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class GetTransactionStatusRequestExecutor implements AgentExecutor<SSConnection> {

	@Override
	public ResponseMessage execute(SSConnection connMgr, Object message) throws Throwable {
		return new GetTransactionStatusResponse(connMgr.hasActiveTransaction()).success();
	}

}

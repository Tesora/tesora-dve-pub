// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class BeginTransactionRequestExecutor implements AgentExecutor<SSConnection> {

	@Override
	public ResponseMessage execute(SSConnection connMgr, Object message) throws PEException {
		connMgr.userBeginTransaction();
		return new GenericResponse().success();
	}

}

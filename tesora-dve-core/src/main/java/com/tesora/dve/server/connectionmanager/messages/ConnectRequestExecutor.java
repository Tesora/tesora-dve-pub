// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import org.apache.log4j.Logger;

import com.tesora.dve.comms.client.messages.ConnectRequest;
import com.tesora.dve.comms.client.messages.ConnectResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.UserCredentials;

public class ConnectRequestExecutor implements AgentExecutor<SSConnection> {
	private static Logger logger = Logger.getLogger(ConnectRequestExecutor.class);

	@Override
	public ResponseMessage execute(SSConnection connMgr, Object message) throws Throwable {
		ConnectRequest cr = (ConnectRequest) message;
		UserCredentials userCred = new UserCredentials(cr.getUserID(), cr.getPassword(), cr.getIsPlaintext());

		try {
			connMgr.startConnection(userCred);
		} catch (PEException pe) {
			PerHostConnectionManager.INSTANCE.addConnectFailure();
			throw pe;
		}

		if (logger.isDebugEnabled())
			logger.debug("ConnectRequest/c(" + connMgr.getName() + "): connected with " + userCred.toString());

		return new ConnectResponse(connMgr.getName()).success();
	}
}

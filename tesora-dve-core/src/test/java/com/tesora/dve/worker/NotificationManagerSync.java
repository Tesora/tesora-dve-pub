// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.NotificationManager;
import com.tesora.dve.server.connectionmanager.NotificationManagerRequest;
import com.tesora.dve.worker.agent.Envelope;

public class NotificationManagerSync extends NotificationManagerRequest {

	private static final long serialVersionUID = 1L;

	@Override
	public ResponseMessage executeRequest(Envelope e, NotificationManager nm)
			throws PEException {
		return new GenericResponse().success();
	}

	@Override
	public MessageType getMessageType() {
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		return null;
	}

}

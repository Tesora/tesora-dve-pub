// OS_STATUS: public
package com.tesora.dve.worker;

import java.io.Serializable;

import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.server.messaging.WorkerManagerRequest;

/**
 * The WorkerManagerSync message allows the sender to make
 * sure the workermanager has processed all pending messages
 * (such as async ReturnWorkerRequest messages)
 */
public class WorkerManagerSync extends WorkerManagerRequest implements
		Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public ResponseMessage executeRequest(Envelope e, WorkerManager wm)
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

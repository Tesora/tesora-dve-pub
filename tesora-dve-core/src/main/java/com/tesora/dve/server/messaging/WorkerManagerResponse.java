// OS_STATUS: public
package com.tesora.dve.server.messaging;

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;

public abstract class WorkerManagerResponse extends ResponseMessage {

	public WorkerManagerResponse() {
	}

	public WorkerManagerResponse(PEException e) {
		super(e);
	}

	private static final long serialVersionUID = -8752485033357932319L;

}

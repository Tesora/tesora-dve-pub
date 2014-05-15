// OS_STATUS: public
package com.tesora.dve.server.messaging;

import java.util.Map;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.Worker;

public class GetWorkerResponse extends WorkerManagerResponse {
	
	private static final long serialVersionUID = -7611456135798719746L;

	Map<StorageSite, Worker> theWorkers;

	public GetWorkerResponse(Map<StorageSite, Worker> theWorkers2) {
		this.theWorkers = theWorkers2;
	}

	public GetWorkerResponse(PEException e) {
		super(e);
	}

	public GetWorkerResponse() {
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.WM_GET_WORKER_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

	public Map<StorageSite, Worker> getWorkers() {
		return theWorkers;
	}

	@Override
	public String toString() {
		return PEStringUtils.toString(this.getClass(), (theWorkers == null) ? null : theWorkers.values());
	}
}

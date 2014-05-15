// OS_STATUS: public
package com.tesora.dve.server.messaging;

import java.util.Arrays;
import java.util.Collection;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.comms.client.messages.GenericResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerManager;
import com.tesora.dve.worker.agent.Envelope;

public class ReturnWorkerRequest extends WorkerManagerRequest {

	private static final long serialVersionUID = 717353305221890938L;
	
	Collection<Worker> theWorkers;

	StorageGroup group;
	
	public ReturnWorkerRequest(StorageGroup group, Collection<Worker> collection) {
		this.theWorkers = collection;
		this.group = group;
	}

	@Override
	public ResponseMessage executeRequest(Envelope e, WorkerManager wm) throws PEException {
		wm.returnWorkerList(group, theWorkers);
		return new GenericResponse().success();
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.WM_RETURN_WORKER_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toString() {
		return PEStringUtils.toString(this.getClass(), Arrays.asList(theWorkers));
	}

}

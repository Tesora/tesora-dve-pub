package com.tesora.dve.server.messaging;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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

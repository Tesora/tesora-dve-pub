// OS_STATUS: public
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

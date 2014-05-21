// OS_STATUS: public
package com.tesora.dve.worker;

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

package com.tesora.dve.eventing.events;

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

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.eventing.EventHandler;
import com.tesora.dve.eventing.Request;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class WorkerGroupSubmitEvent extends Request {

	// MappingSolution mappingSolution,
	// final WorkerRequest req, final DBResultConsumer resultConsumer, int senderCount
	
	private final WorkerRequest wrapped;
	private final DBResultConsumer resultConsumer;
	private final int senderCount;
	private final MappingSolution mappingSolution;
	
	public WorkerGroupSubmitEvent(EventHandler requestor,
			MappingSolution mappingSolution,
			WorkerRequest orig, DBResultConsumer resultConsumer, int senderCount) {
		super(requestor);
		this.wrapped = orig;
		this.resultConsumer = resultConsumer;
		this.senderCount = senderCount;
		this.mappingSolution = mappingSolution;
	}

	public WorkerRequest getWrappedRequest() {
		return wrapped;
	}
	
	public DBResultConsumer getResultConsumer() {
		return resultConsumer;
	}
	
	public int getSenderCount() {
		return senderCount;
	}
	
	public MappingSolution getMappingSolution() {
		return mappingSolution;
	}
	
}

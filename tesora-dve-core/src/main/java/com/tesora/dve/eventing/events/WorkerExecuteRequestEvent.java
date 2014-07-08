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
import com.tesora.dve.eventing.EventSource;
import com.tesora.dve.eventing.Request;
import com.tesora.dve.worker.Worker;

public class WorkerExecuteRequestEvent extends Request {

	private final Worker worker;
	private final DBResultConsumer consumer;
	
	public WorkerExecuteRequestEvent(EventSource origState, Request causedBy, Worker w, DBResultConsumer consumer) {
		super(origState, causedBy);
		worker = w;
		this.consumer = consumer;
	}
	
	public Worker getWorker() {
		return worker;
	}
	
	public DBResultConsumer getConsumer() {
		return consumer;
	}
	
}

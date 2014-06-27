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

public class WorkerRequestEvent extends Request {

	private final WorkerRequest wrapped;
	private final DBResultConsumer consumer;

	public WorkerRequestEvent(EventHandler requestor, WorkerRequest orig, DBResultConsumer consumer) {
		super(requestor);
		this.wrapped = orig;
		this.consumer = consumer;
	}

	public WorkerRequest getWrappedRequest() {
		return wrapped;
	}
	
	public DBResultConsumer getConsumer() {
		return consumer;
	}
	
	
}

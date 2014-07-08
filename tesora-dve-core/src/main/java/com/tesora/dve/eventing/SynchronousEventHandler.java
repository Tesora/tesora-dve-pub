package com.tesora.dve.eventing;

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

import com.tesora.dve.eventing.events.AcknowledgeResponse;
import com.tesora.dve.eventing.events.ExceptionResponse;
import com.tesora.dve.exceptions.PEException;

// this is a transition class - you specify a target and a request,
// and it will block on the call call until it has a response.  if the response
// is an acknowledge, it will just return; if it is an exception it will throw the first
// exception.
public class SynchronousEventHandler implements EventHandler {

	private Response response = null;
	
	public SynchronousEventHandler() {
	}
	
	@Override
	public Response request(Request in) {
		throw new EventingException("Invalid request: SynchronousEventHandler does not handle requests");
	}

	@Override
	public void response(Response in) {
		synchronized(this) {
			response = in;
			notify();
		}
	}

	public void call(Request req, EventHandler target) throws PEException {
		synchronized(this) {
			req.setTarget(this);
			response = target.request(req);
			while(response == null) try {
				wait();
			} catch (InterruptedException ie) {
				// ignore
			}
		}
		if (response.isError()) {
			ExceptionResponse er = (ExceptionResponse) response;
			Throwable first = er.getExceptions().get(0);
			if (first instanceof PEException)
				throw (PEException)first;
			else
				throw new PEException(first);
		} else if (response instanceof AcknowledgeResponse) {
			// ok
		} else {
			throw new PEException("SynchronousEventHandler received response of type " + response.getClass().getName());
		}
	}
	
}

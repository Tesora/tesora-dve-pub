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

// on request we have three return options
// invoke other
// invoke self
// response.  this should pop the state stack
//
// on response we have three return options
// do nothing (i.e. counting)
// response.  this should pop the state stack
// next item
public abstract class SequentialStep implements EventSource {
	
	protected Request generatedRequest = null;
	
	public abstract SequentialTransition onExecute(EventStateMachine esm);
	
	public SequentialTransition execute(EventStateMachine esm) {
		SequentialTransition st = onExecute(esm);
		if (st.getEvents().size() == 1 && st.getEvents().get(0).getEvent().isRequest())
			generatedRequest = (Request) st.getEvents().get(0).getEvent();
		return st;
	}
	
	public abstract SequentialTransition onResponse(Request originalRequest, EventStateMachine esm, Response response, Request forRequest);
	
	protected SequentialTransition propagateRequestException(Request request, Throwable t) {
		return new SequentialTransition()
		.withSequentialEvent(new ExceptionResponse(this,request,t),request.getResponseTarget())
		.withSequentialTransition(null, StackAction.POP);
	}

	// this is a standard propagate - we unpack the incoming event and return a new appropriate event
	protected SequentialTransition propagateResponse(EventStateMachine esm, Request request, AbstractEvent event) {
		SequentialTransition result = new SequentialTransition()
			.withSequentialTransition(null, StackAction.POP);
		Response resp = (Response) event;
		if (resp.isError()) {
			ExceptionResponse er = (ExceptionResponse) resp;
			result.withEvent(new ExceptionResponse(this,request,er), request.getResponseTarget());
		} else {
			result.withEvent(new AcknowledgeResponse(this,request),request.getResponseTarget());
		}
		return result;
	}

}
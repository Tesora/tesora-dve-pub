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

public abstract class AbstractReqRespState implements State {

	@Override
	public StateRole getRole() {
		return StateRole.REQRESP;
	}

	// this is a standard propagate - we unpack the incoming event and return a new appropriate event
	protected TransitionResult propagateResponse(EventStateMachine esm, Request request, AbstractEvent event) {
		TransitionResult result = new TransitionResult()
			.withStateTransition(null, StackAction.POP);
		Response resp = (Response) event;
		if (resp.isError()) {
			ExceptionResponse er = (ExceptionResponse) resp;
			result.withEvent(new ExceptionResponse(this,request,er), request.getResponseTarget());
		} else {
			result.withEvent(new AcknowledgeResponse(this,request),request.getResponseTarget());
		}
		return result;
	}
	
	protected TransitionResult propagateRequestException(Request request, Throwable t) {
		return new TransitionResult()
		.withStateTransition(null, StackAction.POP)
		.withEvent(new ExceptionResponse(this,request,t),request.getResponseTarget());
	}
}

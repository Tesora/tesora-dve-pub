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

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.eventing.events.AcknowledgeResponse;
import com.tesora.dve.eventing.events.ExceptionResponse;
import com.tesora.dve.eventing.events.WrappedResponseMessageEvent;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

// this is a transitional state, it executes via host.submit
public abstract class TransitionReqRespState extends AbstractReqRespState {

	Request request;
	
	@Override
	public TransitionResult onEvent(final EventStateMachine esm, AbstractEvent event) {
		if (event.isRequest()) {
			request = (Request) event;
			final Request reqEvent = request;
			final ChildRunnable child = buildRunnable(esm,request);
			final EventSource src = this;
			// in the runnable I will effectively be acting like another event handler, but in order
			// to do that I need to set up the response target correctly.  I cannot use the one in request,
			// because the response target in request is for my parent caller.
			Singletons.require(HostService.class).execute(new Runnable() {

				@Override
				public void run() {
					// assume it will work, override if it doesn't
					Response resp = null;
					ResponseMessage any = null;
					try {
						any = child.run();
					} catch (Throwable t) {
//						t.printStackTrace();
						resp = new ExceptionResponse(src,reqEvent,t);
					}
					if (resp == null) {
						if (any == null)
							resp = new AcknowledgeResponse(src,reqEvent);
						else
							resp = new WrappedResponseMessageEvent(src,reqEvent,any);
					}
					esm.response(resp);
				}
			});
			return new TransitionResult();
		} else {
			return propagateResponse(esm,request,event);
		}
	}

	protected abstract ChildRunnable buildRunnable(EventStateMachine ems, Request req);
	
	public interface ChildRunnable {
		
		public ResponseMessage run() throws Throwable;
		
	}
}

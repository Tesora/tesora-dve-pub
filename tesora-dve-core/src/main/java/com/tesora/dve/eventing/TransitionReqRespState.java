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
			Singletons.require(HostService.class).execute(new Runnable() {

				@Override
				public void run() {
					// assume it will work, override if it doesn't
					Response resp = new AcknowledgeResponse(reqEvent);
					try {
						child.run();
					} catch (Throwable t) {
						resp = new ExceptionResponse(reqEvent,t);
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
		
		public void run() throws Throwable;
		
	}
}

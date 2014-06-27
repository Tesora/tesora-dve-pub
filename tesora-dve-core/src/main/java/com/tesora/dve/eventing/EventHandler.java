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

// the public interface; implementors can do their own implementation or use
// the EventStateMachine
public interface EventHandler {

	public Response request(Request in);
	
	public void response(Response in);

	// during the transition period we will have event handlers that execute synchronously
	// (i.e. they block).  for such handlers we need to send events to them via Host.submit so
	// that the parent handler can continue to execute asynchronously.
	public boolean isAsynchronous();

	// transitional only - do a request that only responds via the callback, no immediate response
	public void requestCallbackOnly(Request in);
	

}

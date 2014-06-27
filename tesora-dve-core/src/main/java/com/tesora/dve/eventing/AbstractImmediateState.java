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

public abstract class AbstractImmediateState implements State {

	@Override
	public TransitionResult onEvent(EventStateMachine esm, AbstractEvent event) {
		Request req = (Request) event;
		Response out = generateResponse(req);
		State nextState = getNextState(req);
		TransitionResult result = new TransitionResult();
		if (nextState != null)
			result.withStateTransition(nextState, StackAction.REPLACE);
		else
			result.withStateTransition(null, StackAction.POP);
		result.withTargetEvent(out, req.getRequestor());
		return result;
	}

	@Override
	public StateRole getRole() {
		return StateRole.IMMEDIATE;
	}
	
	// build the response message
	public abstract Response generateResponse(Request in);
	
	// get the next state (if any).  if none, we simply pop the state stack.
	public abstract State getNextState(Request in);
	
}

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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryPredicate;

// the transition result is what happens upon some input to the
// current state.  the input is either a request or a response.  we have
// the following results:
//
// In the following a StackRequest is a new state + a stack operation
// The stack operations are: push, replace, pop.
// The new state may be null.
//
// for requests:
//   {StackRequest,Response}:
//      the request was immediately handled
//   {StackRequest,Request}
//      the request is handled by calling someone else
//      we will push the new state if the target is elsewhere, otherwise
//      call into the new state directly
//
// for responses:
//   {StackRequest,Response}:
//      modify the stack as indicated, send the response to the appropriate handler
//      if the target is self, check the top of the stack for the target and call inline
//   {StackRequest,null}:
//      modify the stack as indicated (could have a null stack request, which means do nothing)
//

public class TransitionResult {

	private StackRequest transition;
	List<TargetEvent> events;
	
	public TransitionResult() {
		events = new ArrayList<TargetEvent>();
	}

	public TransitionResult withStateTransition(State nextState, StackAction action) {
		transition = new StackRequest(nextState,action);
		return this;
	}
	
	public TransitionResult withEvent(AbstractEvent ae, EventHandler target) {
		events.add(new TargetEvent(ae,target));
		return this;
	}
	
	public List<TargetEvent> getEvents() {
		return events;
	}

	public StackRequest getStackRequest() {
		return transition;
	}
	
	public boolean isSelfInvoke(final EventStateMachine esm) {
		return Functional.any(events, new UnaryPredicate<TargetEvent>() {

			@Override
			public boolean test(TargetEvent object) {
				return object.getTarget() == esm;
			}
			
		});
	}
	
}

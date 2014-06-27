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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


// we have one instance of an EventStateMachine per logical state machine
// the event state machine is stateless - it's role is to apply the rules to
// the adapted state machine and guide the transitions
public abstract class EventStateMachine implements EventHandler {

	private final String name;
	
	private LinkedList<State> stack;
	
	public EventStateMachine(String name,State initialState) {
		this.name = name;
		this.stack = new LinkedList<State>();
		this.stack.addFirst(initialState);
	}
	
	protected final synchronized Response onEvent(AbstractEvent in, boolean responseOnly) {
		// we could cycle through a few states while processing this event if
		// we call out to other state machines and they immediately fulfill.  accordingly
		// this is implemented as a loop.
		boolean isRequest = in.isRequest();
		List<AbstractEvent> e = Collections.singletonList((AbstractEvent)in);

		do {
			for(Iterator<AbstractEvent> iter = e.iterator(); iter.hasNext();) {
				State current = getState();
				AbstractEvent ae = iter.next();
				TransitionResult transition = current.onEvent(this,ae);
				if (transition.getStackRequest() != null)
					modifyStack(transition.getStackRequest());
				if (transition.isSelfInvoke(this)) {
					if (transition.getEvents().size() > 1)
						throw new EventingException("Bad self invoke: more than one event");
					if (e.size() > 1)
						throw new EventingException("Bad self invoke: more than one initiator");
					e = new ArrayList<AbstractEvent>();
					e.add(transition.getEvents().get(0).getEvent());
					continue;
				}					
				// if we have a next input event, we cannot have a new output event
				// we also cannot have a new state - must have a null stack request
				if (iter.hasNext()) {
					if (!transition.getEvents().isEmpty())
						throw new EventingException("Bad accumulation implementation: have next input event AND next output event");
					if (transition.getStackRequest() != null)
						throw new EventingException("Bad accumulation implementation: have next input event AND new state");
					continue;
				}
				// last iteration through here; process any follow on events
				// we can have at most one response, but we could have multiple requests.
				e = new ArrayList<AbstractEvent>();
				List<TargetEvent> responses = new ArrayList<TargetEvent>();
				for(TargetEvent te : transition.getEvents()) {
					if (te.getEvent().isRequest()) {
						Response anything = te.sendRequest();
						if (anything != null)
							e.add(anything);
					} else {
						responses.add(te);
					}
				}
				if (responses.size() > 1)
					throw new EventingException("Bad state implementation: multiple responses");
				if (responses.size() == 1) {
					if (!e.isEmpty()) {
						throw new EventingException("Bad state implementation: both responses and follow on requests");
					}
					if (isRequest && !responseOnly)
						return responses.get(0).getResponse();
					else
						responses.get(0).sendResponse();
				}
			}
		} while (!e.isEmpty());

		return null;

	}
	
	public final Response request(Request in) {
		return onEvent(in,false);
	}
		
	public final void response(Response in) {
		onEvent(in,false);
	}

	public final void requestCallbackOnly(Request in) {
		onEvent(in,true);
	}
	
	public State getState() {
		return stack.getFirst();
	}
	
	public void modifyStack(StackRequest trans) {
		if (trans == null) return;
		switch(trans.getAction()) {
		case PUSH:
			stack.addFirst(trans.getState());
			break;
		case POP:
			stack.removeFirst();
			break;
		case REPLACE:
			if (!stack.isEmpty())
				stack.removeFirst();
			stack.addFirst(trans.getState());
			break;
		default:
			throw new EventingException("Unknown stack action: " + trans.getAction());
		}
	}
	
}

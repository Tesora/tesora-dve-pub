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
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import com.tesora.dve.eventing.events.ExceptionResponse;


// we have one instance of an EventStateMachine per logical state machine
// the event state machine is stateless - it's role is to apply the rules to
// the adapted state machine and guide the transitions
public class EventStateMachine implements EventHandler {

	private final String name;
	
	private ConcurrentHashMap<Request,StateStack> stacks;
	private final State initialState;	
	
	public EventStateMachine(String name,State initialState) {
		this.name = name;
		this.initialState = initialState;
		this.stacks = new ConcurrentHashMap<Request,StateStack>();
	}

	protected final Response onEvent(AbstractEvent in) {		
		// we could cycle through a few states while processing this event if
		// we call out to other state machines and they immediately fulfill.  accordingly
		// this is implemented as a loop.
		boolean isRequest = in.isRequest();
		List<AbstractEvent> e = Collections.singletonList((AbstractEvent)in);

		do {
			for(Iterator<AbstractEvent> iter = e.iterator(); iter.hasNext();) {
				AbstractEvent ae = iter.next();
				StateStack myStack = getStack(ae);
				if (myStack == null)
					return stackError(ae);
				State current = myStack.getFirst(); 
				TransitionResult transition = current.onEvent(this,ae);
				if (transition.getStackRequest() != null) {
					if (myStack.modifyStack(transition.getStackRequest())) {
						/*
						System.out.println("In state: " + current.getName() + " removing stack " + myStack 
								+ " upon event " + ae.getHistory()
								+ (ae != in ? " kickoff event " + in.getHistory() : ""));
								*/
						stacks.remove(myStack.getRootRequest());
					}
				}
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
						Response anything = null;
						if (te.getTarget() == this) {
							// self invoke; we mostly just set up the state stack, now we're going to try again
							anything = onEvent(te.getEvent());
						} else {
							// invoke elsewhere, set up the reply to
							Request req = (Request) te.getEvent();
							req.setTarget(this);
							/*
							req.setContext("Inbound event: " + ae + PEConstants.LINE_SEPARATOR
									+ ", stack: " + myStack + PEConstants.LINE_SEPARATOR
									+ ", state: " + current.getName());
									*/
							anything = te.getTarget().request(req);
						}
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
					TargetEvent te = responses.get(0);
					if (isRequest)
						return te.getResponse();
					else {
						te.getTarget().response(te.getResponse());
					}
				}
			}
		} while (!e.isEmpty());

		return null;

	}
	
	private Response stackError(AbstractEvent ae) {
		StringBuilder buf = new StringBuilder();
		buf.append("SM: ").append(name);
		buf.append(" stacks{");
		boolean first = true;
		for (Enumeration<Request> kiter = stacks.keys(); kiter.hasMoreElements();) {
			Request kr = kiter.nextElement();
			if (first) first = false;
			else buf.append(",");
			buf.append(System.identityHashCode(kr)).append("@").append(kr.getClass().getSimpleName());
		}
		buf.append("}").append(" Event {").append(ae.getHistory()).append("}");
		// this is a fatal error, so we're just going to forward the error to the caller
		Request root = null;
//		String context = null;
		if (ae.isRequest()) {
			root = (Request) ae;
		} else {
			root = ((Response)ae).getRequest();
//			context = root.getContext();
		}
		EventingException ee = new EventingException("Missing state stack: " + buf.toString()/* + "; context: " + context*/);
		ee.printStackTrace();
		root = root.getCause();
		ExceptionResponse er = new ExceptionResponse(null,root,ee);
		if (ae.isRequest())
			return er;
		else 
			root.getResponseTarget().response(er);
		return null;		
	}
	
	public final Response request(Request in) {
		return onEvent(in);
	}
		
	public final void response(Response in) {
		onEvent(in);
	}

	public StateStack getStack(AbstractEvent ae) {
		StateStack stack = null;
		if (ae.isRequest()) {
			Request r = (Request) ae;
			stack = stacks.get(r);
			if (stack == null) {
				stack = new StateStack(r,initialState);
				stacks.put(r,stack);
			}
		} else {
			// for responses, search the causal chain for a hit
			Response r = (Response) ae;
			Request req = r.getRequest();
			while(stack == null && req != null) {
				stack = stacks.get(req);
				if (stack == null)
					req = req.getCause();
			}
		}
		return stack;
	}
}

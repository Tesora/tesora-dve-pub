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

import com.tesora.dve.eventing.events.ExceptionResponse;

public abstract class SequentialState implements State {
	
	protected final SequentialExecutionMode executionMode;
	protected SequentialStep current;
	protected Request original;
	
	public SequentialState(SequentialExecutionMode mode) {
		executionMode = mode;
	}

	protected abstract SequentialStep buildFirstStep(EventStateMachine esm, Request request);
	
	@Override
	public TransitionResult onEvent(EventStateMachine esm,AbstractEvent event) {
		if (event.isRequest()) {
			Request req = (Request) event;
			if (original != null)
				return new TransitionResult()
					.withStateTransition(null, StackAction.POP)
					.withTargetEvent(new ExceptionResponse(req,new EventingException("Duplicate request to sequential state")), req.getRequestor());
			original = req;
			current = buildFirstStep(esm,original);
			SequentialTransition stepResult = current.execute(esm);
			// since first half can never indicate another step to execute, return the result as is
			return stepResult;
		} else {
			SequentialTransition stepResult = current.onResponse(esm, (Response) event);
			SequentialStep next = stepResult.getNextStep();
			if (next != null) {
				if (stepResult.getStackRequest() != null) {
					// changing state stack with remaining steps - this is an error
					return new TransitionResult()
						.withStateTransition(null, StackAction.POP)
						.withTargetEvent(new ExceptionResponse(original,new EventingException("Remaining sequential step but changing state")), original.getRequestor());
				}
				current = next;
			}
			return stepResult;
		}
	}

	@Override
	public StateRole getRole() {
		return StateRole.SEQUENTIAL;
	}

	// on request we have three return options
	// invoke other
	// invoke self
	// response.  this should pop the state stack
	//
	// on response we have three return options
	// do nothing (i.e. counting)
	// response.  this should pop the state stack
	// next item
	public interface SequentialStep {
		
		public SequentialTransition execute(EventStateMachine esm);
		
		public SequentialTransition onResponse(EventStateMachine esm, Response req);
		
	}
	
	public class SequentialTransition extends TransitionResult {
		
		// the only different thing we could add is the next item
		private SequentialStep nextStep;
		
		public SequentialTransition() {
			super();
		}
		
		public SequentialTransition withNextStep(SequentialStep ss) {
			nextStep = ss;
			return this;
		}
		
		public SequentialStep getNextStep() {
			return nextStep;
		}
		
	}
	
}

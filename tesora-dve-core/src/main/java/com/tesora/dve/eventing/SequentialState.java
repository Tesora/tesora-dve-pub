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
					.withEvent(new ExceptionResponse(this,req,
							new EventingException("Duplicate request to sequential state")), 
							req.getResponseTarget());
			original = req;
			current = buildFirstStep(esm,original);
			boolean done = false;
			SequentialTransition stepResult = null;
			while(!done) {
				stepResult = current.execute(esm);
				// if the first transition is to go directly to the next step, with no event, do that now
				if (stepResult.getNextStep() != null && stepResult.getEvents().isEmpty()) {
					current = stepResult.getNextStep();
				} else {
					done = true;
				}
			}
			return stepResult;
		} else {
			SequentialTransition stepResult = current.onResponse(original, esm, (Response) event, current.generatedRequest);
			// so we can have a few cases here:
			// we have a next step, but no event - just execute the next step
			// we can have no next step, and an event - report back
			// it is not permissible to have a next step & an event
			SequentialStep next = stepResult.getNextStep();
			boolean haveEvent = !stepResult.getEvents().isEmpty();
			if (haveEvent && next != null) 
				// internal error
				return buildInternalError("Internal error: sequential state has next step and event");
			if (next != null && stepResult.getStackRequest() != null)
				return buildInternalError("Internal error: Remaining sequential step but changing state");
			while(next != null) {
				current = next;
				// we have to do the execute side - we have just processed a response for this step, which has 
				// lead to another step - for which we must now execute
				stepResult = current.execute(esm);
				next = stepResult.getNextStep();
			}
			return stepResult;
		}
	}

	@Override
	public StateRole getRole() {
		return StateRole.SEQUENTIAL;
	}

	private TransitionResult buildInternalError(String message) {
		return new TransitionResult()
			.withStateTransition(null, StackAction.POP)
			.withEvent(new ExceptionResponse(this,original,
					new EventingException(message)),
					original.getResponseTarget());
	}
	
}

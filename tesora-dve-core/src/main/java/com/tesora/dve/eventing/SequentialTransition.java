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
	
	public SequentialTransition withSequentialEvent(AbstractEvent ae, EventHandler target) {
		return (SequentialTransition)withEvent(ae,target);
	}

	public SequentialTransition withSequentialTransition(State nextState, StackAction action) {
		return (SequentialTransition)withStateTransition(nextState,action);
	}
	
}

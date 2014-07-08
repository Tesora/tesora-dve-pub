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

import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class DispatchState implements State {
	
	ListOfPairs<Class<?>,State> matching;
	final String name;
	
	public DispatchState(String n) {
		matching = new ListOfPairs<Class<?>,State>();
		name = n;
	}
	
	public DispatchState withMatch(Class<?> eventClass, State targetState) {
		matching.add(eventClass,targetState);
		return this;
	}
	
	@Override
	public TransitionResult onEvent(EventStateMachine esm, AbstractEvent event) {
		State actual = getTarget(esm, event);
		if (actual == null)
			throw new EventingException("No target state found for " + event.getClass().getName());
		return new TransitionResult()
			.withStateTransition(actual, StackAction.PUSH)
			.withEvent(event, esm);
	}

	public State getTarget(EventStateMachine esm, AbstractEvent event) {
		Class<?> ec = event.getClass();
		for(Pair<Class<?>,State> p : matching) {
			if (p.getFirst().equals(ec))
				return p.getSecond();
		}
		return null;
	}

	@Override
	public StateRole getRole() {
		return StateRole.DISPATCH;
	}

	@Override
	public String getName() {
		return name;
	}
}
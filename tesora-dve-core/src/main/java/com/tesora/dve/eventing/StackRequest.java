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

public class StackRequest {

	State newState;
	StackAction action;
	
	public StackRequest(State nextState, StackAction action) {
		this.newState = nextState;
		this.action = action;
		if (action == null)
			throw new EventingException("Must have stack action");
	}

	public StackAction getAction() {
		return action;
	}
	
	public State getState() {
		return newState;
	}
		
}

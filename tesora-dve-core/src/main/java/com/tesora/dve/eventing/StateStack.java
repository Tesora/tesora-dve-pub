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

import java.util.LinkedList;

public class StateStack {

	private final LinkedList<State> stack;
	private final Request root;
	
	public StateStack(Request root, State initial) {
		stack = new LinkedList<State>();
		stack.push(initial);
		this.root = root;
	}
	
	public State getFirst() {
		return stack.getFirst();
	}
	
	public Request getRootRequest() {
		return root;
	}
	
	public boolean modifyStack(StackRequest trans) {
		if (trans == null) return false;
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
		return (stack.size() == 1 && trans.getAction() == StackAction.POP); 
	}

	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("StateStack for event [").append(root);
		boolean first = true;
		for(State s : stack) {
			if (first) first = false;
			else buf.append(", ");
			buf.append(s.getName());
		}
		buf.append("]");
		return buf.toString();
	}
}

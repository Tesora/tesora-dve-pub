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

public class TargetEvent {

	private AbstractEvent theEvent;
	private EventHandler theTarget;
	
	public TargetEvent(AbstractEvent e, EventHandler t) {
		theEvent = e;
		theTarget = t;
	}
	
	public AbstractEvent getEvent() {
		return theEvent;
	}
	
	public EventHandler getTarget() {
		return theTarget;
	}

	// convenient
	public void sendResponse() {
		theTarget.response((Response) theEvent);
	}
	
	public Response sendRequest() {
		return theTarget.request((Request) theEvent);
	}
	
	public Response getResponse() {
		return (Response)theEvent;
	}

	public boolean isSelfInvoke(EventStateMachine esm) {
		return (esm == theTarget);
	}
	
}

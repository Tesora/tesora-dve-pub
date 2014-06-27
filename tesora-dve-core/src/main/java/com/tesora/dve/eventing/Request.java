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

public abstract class Request extends AbstractEvent {

	protected Request parentRequest;
	protected final EventHandler requestedBy;
	
	public Request(EventHandler requestor) {
		this.requestedBy= requestor;
		if (requestor == null)
			throw new EventingException("Request constructed with null requestor");
	}
	
	public void setParentRequest(Request p) {
		this.parentRequest = p;
	}
	
	public EventHandler getRequestor() {
		return requestedBy;
	}
	
	public Request getCausedBy() {
		return parentRequest;
	}
	
	@Override
	public boolean isRequest() {
		return true;
	}

}

package com.tesora.dve.eventing;

import java.util.List;

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

public class Response extends AbstractEvent {

	protected final Request forRequest;
	
	public Response(EventSource generatingState, Request req) {
		super(generatingState);
		this.forRequest = req;
	}
	
	@Override
	public boolean isRequest() {
		return false;
	}

	public Request getRequest() {
		return forRequest;
	}
	
	public boolean isError() {
		return false;
	}
	
	@Override
	public void collectHistory(List<HistoryEntry> entries, int nestLevel) {
		entries.add(new HistoryEntry(toString(),nestLevel));
		forRequest.collectHistory(entries, nestLevel+1);
	}
	

}

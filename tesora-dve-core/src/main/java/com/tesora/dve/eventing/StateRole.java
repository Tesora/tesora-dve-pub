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

public enum StateRole {

	// dispatch state, responsible for dispatching an inbound request to one of the other states
	DISPATCH,
	// immediate state, always fulfills request immediately without blocking or sending a further event
	IMMEDIATE,
	// represents multisubstate states (i.e. execute a, execute b, send response)
	SEQUENTIAL,
	// a single request/response state (could be multiple responses, but only one conversation)
	REQRESP
	
}

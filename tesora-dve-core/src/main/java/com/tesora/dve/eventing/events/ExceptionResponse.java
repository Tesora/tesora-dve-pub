package com.tesora.dve.eventing.events;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.eventing.EventSource;
import com.tesora.dve.eventing.Request;
import com.tesora.dve.eventing.Response;

public class ExceptionResponse extends Response {

	// acts more like a collector - we could have multiple exceptions
	List<Throwable> exceptions;
	
	public ExceptionResponse(EventSource src, Request targ, Throwable t) {
		super(src, targ);
		exceptions = new ArrayList<Throwable>();
		exceptions.add(t);
	}
	
	public ExceptionResponse(EventSource src, Request targ, List<ExceptionResponse> chained) {
		super(src, targ);
		exceptions = new ArrayList<Throwable>();
		for(ExceptionResponse er : chained)
			exceptions.addAll(er.getExceptions());
	}
	
	public ExceptionResponse(EventSource src, Request targ, ExceptionResponse chained) {
		this(src,targ,Collections.singletonList(chained));
	}
	
	public boolean isError() {
		return true;
	}
	
	public List<Throwable> getExceptions() {
		return exceptions;
	}
}

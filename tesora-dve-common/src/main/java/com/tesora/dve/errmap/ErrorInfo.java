package com.tesora.dve.errmap;

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

public class ErrorInfo {

	private final ErrorCode ec;
	private final Object[] params;

	private StackTraceElement location;
	
	// generics are used here to ensure we have the right parameters
	public <First> ErrorInfo(OneParamErrorCode<First> ec, First arg0) {
		super();
		this.ec = ec;
		params = new Object[] { arg0 };
	}
	
	public <First,Second> ErrorInfo(TwoParamErrorCode<First, Second> ec, First arg0, Second arg1) {
		super();
		this.ec = ec;
		params = new Object[] { arg0, arg1 };
	}
	
	public ErrorInfo(ZeroParamErrorCode ec) {
		super();
		this.ec = ec;
		params = new Object[] {};
	}
	
	public ErrorCode getCode() {
		return ec;
	}
	
	public Object[] getParams() {
		return params;
	}

	public StackTraceElement getLocation() {
		return location;
	}
	
	public ErrorInfo withLocation(StackTraceElement ste) {
		location = ste;
		return this;
	}
	
}

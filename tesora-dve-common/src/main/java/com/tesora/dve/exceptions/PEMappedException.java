package com.tesora.dve.exceptions;

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

import com.tesora.dve.errmap.ErrorInfo;

public class PEMappedException extends PEException implements HasErrorInfo {

	private static final long serialVersionUID = 1L;
	private final ErrorInfo error;
	private boolean hasLocation = false;
	
	
	public PEMappedException(ErrorInfo info) {
		super();
		this.error = info;
		addLocation();
	}

	public PEMappedException(ErrorInfo info, boolean emitLocation) {
		super();
		this.error = info;
		hasLocation = emitLocation;
		addLocation();
	}
	
	public PEMappedException(ErrorInfo info, String message, Throwable cause) {
		super(message, cause);
		this.error = info;
		addLocation();
	}

	public PEMappedException(ErrorInfo info, String message) {
		super(message);
		this.error = info;
		addLocation();
	}

	public PEMappedException(ErrorInfo info, Throwable cause) {
		super(cause);
		this.error = info;
		addLocation();
	}

	public ErrorInfo getErrorInfo() {
		return this.error;
	}
	
	public boolean hasLocation() {
		return hasLocation;
	}

	public void setHasLocation(boolean v) {
		hasLocation = v;
	}
	
	protected void addLocation() {
		if (this.error != null && hasLocation()) {
			this.error.withLocation(getStackTrace()[0]);
		}
	}

}

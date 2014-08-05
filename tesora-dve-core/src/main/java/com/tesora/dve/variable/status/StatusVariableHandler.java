package com.tesora.dve.variable.status;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;


public abstract class StatusVariableHandler {

	private final String variableName;
	
	public StatusVariableHandler(String name) {
		this.variableName = name;
	}
	
	public String getName() {
		return variableName;
	}
	
	public final String getValue() throws PEException {
		try {
			return getValueInternal();
		} catch (Throwable t) {
			throw new PENotFoundException("Unable to find value for status variable " + getName(), t);
		}
	}

	public final void reset() throws PEException {
		try {
			resetValueInternal();
		} catch (Throwable t) {
			throw new PEException("Unable to reset value for status variable " + getName(), t);
		}
	}
	
	protected abstract String getValueInternal() throws Throwable;
	
	protected void resetValueInternal() throws Throwable {
		
	}
}

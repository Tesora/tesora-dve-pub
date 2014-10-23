package com.tesora.dve.variables;

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

import java.util.Collection;
import java.util.LinkedHashMap;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;

public class ScopedVariables {

	private final String scopeName;
	
	private final LinkedHashMap<String,ScopedVariableHandler> handlers;
	
	public ScopedVariables(String scopeName, ScopedVariableHandler...configured) {
		this.scopeName = scopeName;
		this.handlers = new LinkedHashMap<String,ScopedVariableHandler>();
		for(ScopedVariableHandler svh : configured)
			handlers.put(svh.getName(),svh);
	}
	
	public String getScopeName() {
		return scopeName;
	}
	
	public ScopedVariableHandler getScopedVariableHandler(String name) throws PEException {
		ScopedVariableHandler svh = handlers.get(name);
		if (svh == null)
			throw new PENotFoundException(String.format("Variable \"%s\" not found", name));
		return svh;
	}
	
	public Collection<ScopedVariableHandler> getHandlers() {
		return handlers.values();
	}
}

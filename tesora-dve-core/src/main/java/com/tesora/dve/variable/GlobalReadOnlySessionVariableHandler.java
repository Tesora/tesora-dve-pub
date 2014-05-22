package com.tesora.dve.variable;

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

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;

public class GlobalReadOnlySessionVariableHandler extends SessionVariableHandler {

	String globalVariableName;
	String value;
	
	@Override
	public void setValue(SSConnection ssCon, String variableName, String value)
			throws PEException {
		throw new PEException("Variable '" + variableName + "' not settable as session variable");
	}

	@Override
	public void initialise(String variableName, String defaultValue) {
		globalVariableName = defaultValue;
	}

	@Override
	public String getValue(SSConnection ssCon, String name) throws PEException {
		if (value != null) return value;
		try {
            value = Singletons.require(HostService.class).getGlobalVariable(ssCon.getCatalogDAO(), globalVariableName);
			return value;
		} catch (Exception e) {
			throw new PECodingException("Session variable \"" + name + "\" maps to undefined DVE variable \""
					+ globalVariableName + "\"", e);
		}
	}
	
	@Override
	public boolean defaultValueRequiresConnection() {
		return true;
	}

}

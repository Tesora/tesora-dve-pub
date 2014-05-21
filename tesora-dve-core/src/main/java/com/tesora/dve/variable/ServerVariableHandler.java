// OS_STATUS: public
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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public class ServerVariableHandler extends GlobalVariableHandler {

	String hostVariableName;
	
	public void initialise(String name, String defaultValue) {
		hostVariableName = defaultValue;
	}

	@Override
	public void setValue(CatalogDAO c, String name, String value)
			throws PENotFoundException {
		throw new PENotFoundException("Set not supported on Server Variable Handlers");
	}

	@Override
	public String getValue(CatalogDAO c, String name)
			throws PENotFoundException {
		try {
            return BeanVariableHandler.callGetValue(Singletons.require(HostService.class), hostVariableName);
		} catch (PENotFoundException e) {
			throw new PENotFoundException("Unable to find value for server variable " + name, e);
		}
	}
}

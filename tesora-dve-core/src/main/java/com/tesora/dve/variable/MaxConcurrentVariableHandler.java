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
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.MySqlPortalService;
import com.tesora.dve.singleton.Singletons;

public class MaxConcurrentVariableHandler extends ConfigVariableHandler {
	
	public static final int MAX_CONCURRENT = 5000;

	@Override
	public void setValue(CatalogDAO c, String name, String value) throws PEException {
		VariableValueConverter.toInternalInteger(value,  1,  MAX_CONCURRENT);

		super.setValue(c, name, value);
	}

	@Override
	public void onValueChange(String variableName, String newValue) throws PEException {
		int maxConcurrent = VariableValueConverter.toInternalInteger(newValue,  1,  MAX_CONCURRENT);
		
		// onValueChange will be called at startup before the portal has been created
        MySqlPortalService portal = Singletons.lookup(MySqlPortalService.class);
        if(portal != null)
			portal.setMaxConcurrent(maxConcurrent);
	}
}

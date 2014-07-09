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
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;

public class ConfigVariableHandler extends GlobalVariableHandler {
	
	String variableName;
	boolean global;
	
	public void initialise(CatalogDAO c, String name, String defaultValue, boolean global) throws Throwable {
		GlobalConfig configVar = c.findConfig(name, false); 
		if (configVar == null)
			configVar = c.createConfig(name, defaultValue);
		this.variableName = name;
		this.global = global;
		onValueChange(configVar.getName(), configVar.getValue());
	}
	
	@Override
	public void setValue(final CatalogDAO c, final String name, final String value) throws PEException  {
		try {
			c.new EntityUpdater() {
				@Override
				public CatalogEntity update() throws Throwable {
					GlobalConfig configEntity = c.findConfig(name);
					configEntity.setValue(value);
					return configEntity;
				}
			}.execute();
		} catch (Throwable e) {
			throw new PEException("Cannot set variable " + name, e);
		}
	}
	
	@Override
	public String getValue(CatalogDAO c, String name) throws PENotFoundException {
		return c.findConfig(name).getValue();
	}

}

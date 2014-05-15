// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;

public class ConfigVariableHandler extends GlobalVariableHandler {
	
	String variableName;

	public void initialise(CatalogDAO c, String name, String defaultValue) throws Throwable {
		GlobalConfig configVar = c.findConfig(name, false); 
		if (configVar == null)
			configVar = c.createConfig(name, defaultValue);
		this.variableName = name;
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

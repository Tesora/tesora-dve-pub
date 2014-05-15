// OS_STATUS: public
package com.tesora.dve.variable;

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

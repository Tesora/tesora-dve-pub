// OS_STATUS: public
package com.tesora.dve.variable.status;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.variable.BeanVariableHandler;

public class HostStatusVariableHandler extends StatusVariableHandler {

	@Override
	public String getValue(CatalogDAO c, String name) throws PENotFoundException {
		try {
			// the defaultValue is where the method name to get this variable is stored
            return BeanVariableHandler.callGetValue(Singletons.require(HostService.class), defaultValue);
		} catch (PENotFoundException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}
	
	@Override
	public void reset(CatalogDAO c, String name) throws PENotFoundException {
		try {
            BeanVariableHandler.callResetValue(Singletons.require(HostService.class), defaultValue);
		} catch (PENotFoundException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}
}

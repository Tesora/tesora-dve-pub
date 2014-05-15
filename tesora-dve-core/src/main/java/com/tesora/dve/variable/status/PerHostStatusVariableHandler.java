// OS_STATUS: public
package com.tesora.dve.variable.status;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.variable.BeanVariableHandler;

public class PerHostStatusVariableHandler extends StatusVariableHandler {

	@Override
	public String getValue(CatalogDAO c, String name) throws PENotFoundException {
		try {
			return BeanVariableHandler.callGetValue(PerHostConnectionManager.INSTANCE, defaultValue);
		} catch (PENotFoundException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}
	
	@Override
	public void reset(CatalogDAO c, String name) throws PENotFoundException {
		try {
			BeanVariableHandler.callResetValue(PerHostConnectionManager.INSTANCE, defaultValue);
		} catch (PENotFoundException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}
}

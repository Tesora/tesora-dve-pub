// OS_STATUS: public
package com.tesora.dve.variable.status;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.variable.BeanVariableHandler;

public class JVMStatusVariableHandler extends StatusVariableHandler {

	@Override
	public String getValue(CatalogDAO c, String name) throws PENotFoundException {
		try {
			return BeanVariableHandler.callGetValue(this, defaultValue);
		} catch (PENotFoundException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}
	
	@Override
	public void reset(CatalogDAO c, String name) throws PENotFoundException {
	}
	
	public long getMemoryMax() {
		return Runtime.getRuntime().maxMemory();
	}

	public long getMemoryTotal() {
		return Runtime.getRuntime().totalMemory();
	}

	public long getMemoryFree() {
		return Runtime.getRuntime().freeMemory();
	}

	public long getMemoryUsed() {
		return getMemoryTotal() - getMemoryFree();
	}

	public long getThreadsActive() {
		return Thread.activeCount();
	}
}

// OS_STATUS: public
package com.tesora.dve.variable.status;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.StorageGroup.GroupScale;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.worker.DynamicGroup;

public class RedistStatusVariableHandler extends StatusVariableHandler {

	@Override
	public String getValue(CatalogDAO c, String name) throws PENotFoundException {
		try {
			return Long.toString(DynamicGroup.getCurrentUsage(GroupScale.valueOf(defaultValue)));
		} catch (PEException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}

	@Override
	public void reset(CatalogDAO c, String name) throws PENotFoundException {
		try {
			DynamicGroup.resetCurrentUsage(GroupScale.valueOf(defaultValue));
		} catch (PEException e) {
			throw new PENotFoundException("Unable to reset status variable " + name, e);
		}
	}
}

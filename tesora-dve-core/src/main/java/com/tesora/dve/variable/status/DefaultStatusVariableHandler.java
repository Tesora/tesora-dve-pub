// OS_STATUS: public
package com.tesora.dve.variable.status;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;

public class DefaultStatusVariableHandler extends StatusVariableHandler {

	@Override
	public String getValue(CatalogDAO c, String name) throws PEException {
		return defaultValue;
	}

	@Override
	public void reset(CatalogDAO c, String name) throws PEException {
	}
}

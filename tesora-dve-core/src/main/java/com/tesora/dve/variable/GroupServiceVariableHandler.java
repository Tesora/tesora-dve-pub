// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupManager;

public class GroupServiceVariableHandler extends ConfigVariableHandler {

	@Override
	public void setValue(CatalogDAO c, String name, String value) throws PEException {
		if ( GroupManager.isValidServiceType(value) ) {
			super.setValue(c, name, value);
		} else {
			throw new PEException("Value '" + value + "' is not valid for variable '" + name + "'" );
		}
	}

}

// OS_STATUS: public
package com.tesora.dve.variable;

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

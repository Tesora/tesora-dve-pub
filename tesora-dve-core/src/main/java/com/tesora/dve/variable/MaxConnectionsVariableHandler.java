// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MaxConnectionsVariableHandler extends ConfigVariableHandler {

	public static final int MAX_CONNECTIONS = 5000;
	
	@Override
	public void setValue(CatalogDAO c, String name, String value) throws PEException {
		VariableValueConverter.toInternalInteger(value,  1,  MAX_CONNECTIONS);

		super.setValue(c, name, value);
	}

	@Override
	public void onValueChange(String variableName, String newValue) throws PEException {
		int maxConnections = VariableValueConverter.toInternalInteger(newValue,  1,  MAX_CONNECTIONS);

		SSConnection.setMaxConnections(maxConnections);
	}
}

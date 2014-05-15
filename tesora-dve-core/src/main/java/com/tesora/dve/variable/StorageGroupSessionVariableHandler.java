// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class StorageGroupSessionVariableHandler extends
		GlobalShadowVariableHandler {

	@Override
	public void setValue(SSConnection ssCon, String variableName, String value)
			throws PEException {
		ssCon.getCatalogDAO().findPersistentGroup(value, /* check existence */ true);
		super.setValue(ssCon, variableName, value);
	}

}

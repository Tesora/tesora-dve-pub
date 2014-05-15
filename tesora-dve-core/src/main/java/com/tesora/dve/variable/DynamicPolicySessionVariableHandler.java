// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class DynamicPolicySessionVariableHandler extends
		GlobalShadowVariableHandler {

	@Override
	public void setValue(SSConnection ssCon, String variableName, String value)
			throws PEException {
		ssCon.getCatalogDAO().findDynamicPolicy(value, /* check existence */ true);
		super.setValue(ssCon, variableName, value);
	}

}

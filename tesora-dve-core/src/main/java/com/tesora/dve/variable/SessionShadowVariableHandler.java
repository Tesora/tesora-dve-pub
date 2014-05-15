// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class SessionShadowVariableHandler extends
		GlobalShadowVariableHandler {

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		ssCon.updateWorkerState(getSessionAssignmentClause(name, value));
		super.setValue(ssCon, name, value);
	}
}

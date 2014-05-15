// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class DBSessionVariableHandler extends SessionVariableHandler {

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		ssCon.updateWorkerState(getSessionAssignmentClause(name, value));
		super.setValue(ssCon, name, value);
	}

	@Override
	public String getSessionAssignmentClause(String name, String value) {
		return name + " = '" + value + "'";
	}

}

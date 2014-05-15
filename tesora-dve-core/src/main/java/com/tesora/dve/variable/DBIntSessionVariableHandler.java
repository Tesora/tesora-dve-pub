// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class DBIntSessionVariableHandler extends DBSessionVariableHandler {

	@Override
	public String getSessionAssignmentClause(String name, String value) {
		return name + " = " + value;
	}

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		try {
			Integer.parseInt(value);
		} catch (NumberFormatException e) {
			throw new PEException("Value '" + value + "' for variable \"" + name + "\" is not of the required integer format", e);
		}
		super.setValue(ssCon, name, value);
	}

}

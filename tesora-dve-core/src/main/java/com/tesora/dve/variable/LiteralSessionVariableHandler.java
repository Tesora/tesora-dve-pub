// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

/**
 * Allows the declaration of a variable with a default value that is returned.
 * Throws exception if setValue is called.
 */
public class LiteralSessionVariableHandler extends SessionVariableHandler {

	@Override
	public void setValue(SSConnection ssCon, String name, String value) throws PEException {
		throw new PEException("Variable '" + name + "' not settable as session variable");
	}

	@Override
	public String getValue(SSConnection ssCon, String name) throws PEException {
		return getDefaultValue(ssCon);
	}

}

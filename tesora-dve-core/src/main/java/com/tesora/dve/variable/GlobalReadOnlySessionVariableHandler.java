// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;

public class GlobalReadOnlySessionVariableHandler extends SessionVariableHandler {

	String globalVariableName;
	String value;
	
	@Override
	public void setValue(SSConnection ssCon, String variableName, String value)
			throws PEException {
		throw new PEException("Variable '" + variableName + "' not settable as session variable");
	}

	@Override
	public void initialise(String variableName, String defaultValue) {
		globalVariableName = defaultValue;
	}

	@Override
	public String getValue(SSConnection ssCon, String name) throws PEException {
		if (value != null) return value;
		try {
            value = Singletons.require(HostService.class).getGlobalVariable(ssCon.getCatalogDAO(), globalVariableName);
			return value;
		} catch (Exception e) {
			throw new PECodingException("Session variable \"" + name + "\" maps to undefined DVE variable \""
					+ globalVariableName + "\"", e);
		}
	}
	
	@Override
	public boolean defaultValueRequiresConnection() {
		return true;
	}

}

// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;

public class GlobalShadowVariableHandler extends SessionVariableHandler {
	
	String globalVariableName;

	@Override
	public void setValue(SSConnection ssCon, String variableName, String value)
			throws PEException {
		super.setValue(ssCon, variableName, value);
	}

	@Override
	public void initialise(String variableName, String defaultValue) {
		globalVariableName = defaultValue;
	}

	@Override
	public String getDefaultValue(SSConnection ssCon) throws PEException {
        return Singletons.require(HostService.class).getGlobalVariable(ssCon.getCatalogDAO(), globalVariableName);
	}
	
	@Override
	public boolean defaultValueRequiresConnection() {
		return true;
	}

}

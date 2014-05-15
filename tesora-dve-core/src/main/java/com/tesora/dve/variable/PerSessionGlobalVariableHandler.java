// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;

public class PerSessionGlobalVariableHandler extends SessionVariableHandler {

	@Override
	public String getDefaultValue(SSConnection ssCon) throws PEException {
        return Singletons.require(HostService.class).getGlobalVariable(ssCon.getCatalogDAO(), getVariableName());
	}
}

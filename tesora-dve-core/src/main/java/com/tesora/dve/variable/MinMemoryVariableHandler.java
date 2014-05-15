// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.ConnectionSemaphore;

public class MinMemoryVariableHandler extends ConfigVariableHandler {

	@Override
	public void onValueChange(String variableName, String newValue) throws PEException {
		ConnectionSemaphore.adjustMinMemory(Integer.parseInt(newValue));
	}

}

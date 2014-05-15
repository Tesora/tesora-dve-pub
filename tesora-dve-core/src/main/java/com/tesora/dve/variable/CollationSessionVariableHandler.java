// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public class CollationSessionVariableHandler extends DBSessionVariableHandler {

	public static final String VARIABLE_NAME = "collation_connection";

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		if (VARIABLE_NAME.equalsIgnoreCase(name)) {
			Singletons.require(HostService.class).getDBNative().assertValidCollation(value);
			super.setValue(ssCon, name, value);
		} else {
			throw new PECodingException(this.getClass().getSimpleName() + " called for " + name + " which it cannot handle");
		}
	}
}

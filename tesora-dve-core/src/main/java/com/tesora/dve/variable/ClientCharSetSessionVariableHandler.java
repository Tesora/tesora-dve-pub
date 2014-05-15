// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.mysql.MysqlNativeCharSetCatalog;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class ClientCharSetSessionVariableHandler extends DBSessionVariableHandler {

	public static final String VARIABLE_NAME = "character_set_client";

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		if (VARIABLE_NAME.equalsIgnoreCase(name)) {
			NativeCharSet nativeCharSet = MysqlNativeCharSetCatalog.DEFAULT_CATALOG.findCharSetByName(value, true);
			ssCon.setClientCharSet(nativeCharSet);
			super.setValue(ssCon, name, value);
		} else {
			throw new PECodingException(this.getClass().getSimpleName() + " called for " + name + " which it cannot handle");
		}
	}

}

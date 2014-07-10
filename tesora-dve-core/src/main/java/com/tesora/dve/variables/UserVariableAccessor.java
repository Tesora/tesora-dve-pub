package com.tesora.dve.variables;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class UserVariableAccessor extends AbstractVariableAccessor {

	private final String name;
	
	public UserVariableAccessor(String n) {
		super();
		name = n;
	}
	
	@Override
	public String getValue(VariableStoreSource conn) throws PEException {
		return conn.getUserVariableStore().getValue(name);
	}

	@Override
	public void setValue(VariableStoreSource conn, String v) throws Throwable {
		conn.getUserVariableStore().setValue(name, v);
	}

	@Override
	public String getSQL() {
		return String.format("@%s",name);
	}

}

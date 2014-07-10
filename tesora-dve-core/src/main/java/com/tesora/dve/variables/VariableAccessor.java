package com.tesora.dve.variables;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class VariableAccessor extends AbstractVariableAccessor {

	private final VariableHandler handler;
	private final VariableScope scope;
	
	public VariableAccessor(VariableHandler theHandler, VariableScope theScope) {
		super();
		this.handler = theHandler;
		this.scope = theScope;
	}
	
	@Override
	public String getValue(VariableStoreSource conn) throws PEException {
		return handler.getMetadata().convertToExternal(handler.getValue(conn, scope));
	}

	@Override
	public void setValue(VariableStoreSource conn, String v) throws Throwable {
		handler.setValue(conn, scope, v);
	}

	@Override
	public String getSQL() {
		StringBuilder buf = new StringBuilder();
		if (scope == VariableScope.GLOBAL) {
			buf.append("@@global.");
		} else if (scope == VariableScope.SESSION) {
			buf.append("@@session.");
		}
		buf.append(handler.getName());
		return buf.toString();
	}

}

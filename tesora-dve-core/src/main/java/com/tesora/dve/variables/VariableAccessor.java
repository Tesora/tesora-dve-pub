package com.tesora.dve.variables;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.tesora.dve.exceptions.PEException;

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
		return handler.toExternal(handler.getValue(conn, scope));
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

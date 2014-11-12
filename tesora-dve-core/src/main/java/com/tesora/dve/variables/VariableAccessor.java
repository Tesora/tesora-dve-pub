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
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;

public class VariableAccessor extends AbstractVariableAccessor {

	@SuppressWarnings("rawtypes")
	private final VariableHandler handler;
	private final VariableScope scope;
	
	@SuppressWarnings("rawtypes")
	public VariableAccessor(VariableHandler theHandler, VariableScope theScope) {
		super();
		this.handler = theHandler;
		this.scope = theScope;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public String getValue(VariableStoreSource conn) throws PEException {
		return handler.toExternal(handler.getValue(conn, scope.getKind()));
	}

	@Override
	public void setValue(VariableStoreSource conn, String v) throws Throwable {
		handler.setValue(conn, scope.getKind(), v);
	}

	@Override
	public String getSQL() {
		StringBuilder buf = new StringBuilder();
		if (scope.getKind() == VariableScopeKind.GLOBAL) {
			buf.append("@@global.");
		} else if (scope.getKind() == VariableScopeKind.SESSION) {
			buf.append("@@session.");
		}
		buf.append(handler.getName());
		return buf.toString();
	}

}

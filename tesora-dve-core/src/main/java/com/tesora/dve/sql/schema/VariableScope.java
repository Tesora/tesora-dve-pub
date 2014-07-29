package com.tesora.dve.sql.schema;

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

public class VariableScope {

	protected VariableScopeKind kind;
	protected String scopeName;
	
	// one of the builtin scopes
	public VariableScope(VariableScopeKind vk) {
		kind = vk;
		scopeName = null;
	}
	
	// one of the nonbuiltin scopes
	public VariableScope(String unq) {
		kind = VariableScopeKind.SCOPED;
		scopeName = unq;
	}
	
	public VariableScopeKind getKind() {
		return kind;
	}

	public String getScopeName() {
		return scopeName;
	}

	public boolean isDVEScope() {
		return kind == VariableScopeKind.SCOPED;
	}
	
	public boolean isUserScope() {
		return kind == VariableScopeKind.USER;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((kind == null) ? 0 : kind.hashCode());
		result = prime * result
				+ ((scopeName == null) ? 0 : scopeName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VariableScope other = (VariableScope) obj;
		if (kind != other.kind)
			return false;
		if (scopeName == null) {
			if (other.scopeName != null)
				return false;
		} else if (!scopeName.equals(other.scopeName))
			return false;
		return true;
	}
	
	
}
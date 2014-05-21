// OS_STATUS: public
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

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.variable.VariableScopeKind;

public class VariableScope {

	public enum VariableKind {
		// transient session values (mysql and pe)
		SESSION,
		// transient user session values
		USER,
		// scoped persistent values (dve, <scope> <scope-name>)
		SCOPED,
		// mysql global variables - we detect them only so we can indicate that we don't support them
		GLOBAL;
		
	}

	protected VariableKind kind;
	protected VariableScopeKind scopeKind;
	protected UnqualifiedName scopeName;
	
	public VariableScope(VariableKind vk, String sk, UnqualifiedName sn) {
		kind = vk;
		if (sk != null) {
			scopeKind = VariableScopeKind.lookup(sk);
			if (scopeKind == null)
				throw new SchemaException(Pass.SECOND, "No such scope kind: " + sk);
		} else {
			scopeKind = lookupInternalScopeKind(vk);
		}
		scopeName = sn;
	}
	
	public VariableScope(VariableKind vk) {
		kind = vk;
		scopeKind = lookupInternalScopeKind(kind);
		scopeName = null;
	}
	
	private static VariableScopeKind lookupInternalScopeKind(VariableKind vk) {
		if (vk == VariableKind.USER)
			return VariableScopeKind.USER;
		else if (vk == VariableKind.SESSION)
			return VariableScopeKind.SESSION;
		else if (vk == VariableKind.SCOPED)
			throw new SchemaException(Pass.SECOND, "Must set scope kind");
		return null;
	}
	
	public VariableKind getKind() {
		return kind;
	}

	public VariableScopeKind getScopeKind() {
		return scopeKind;
	}
	
	public String getScopeName() {
		if (scopeKind.hasName() && scopeName != null)			
			return scopeName.get();
		return null;
	}

	public boolean isDveScope() {
		return scopeKind == VariableScopeKind.DVE;
	}
	
	public boolean isUserScope() {
		return scopeKind == VariableScopeKind.USER;
	}
	
	public String getSQL() {
		String candidate = getScopeName();
		if (candidate == null) return scopeKind.name();
		return candidate;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((kind == null) ? 0 : kind.hashCode());
		result = prime * result
				+ ((scopeKind == null) ? 0 : scopeKind.hashCode());
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
		if (scopeKind == null) {
			if (other.scopeKind != null)
				return false;
		} else if (!scopeKind.equals(other.scopeKind))
			return false;
		if (scopeName == null) {
			if (other.scopeName != null)
				return false;
		} else if (!scopeName.equals(other.scopeName))
			return false;
		return true;
	}
	
	
}
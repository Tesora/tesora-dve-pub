// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

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

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.variable.VariableAccessor;

public class VariableInstance extends ExpressionNode {

	protected UnqualifiedName variableName;
	protected VariableScope variableScope;
		
	public VariableInstance(UnqualifiedName varName, VariableScope scope, SourceLocation sloc) {
		super(sloc);
		variableName = varName;
		variableScope = scope;
	}
	
	public UnqualifiedName getVariableName() {
		return variableName;
	}
	
	public VariableScope getScope() {
		return variableScope;
	}
	
	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		return new NameAlias(new UnqualifiedName("var_"));
	}

	public VariableAccessor buildAccessor() {
		return new VariableAccessor(getScope().getScopeKind(), getScope().getScopeName(), variableName.get());
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		VariableInstance nvi = new VariableInstance(variableName, variableScope, getSourceLocation());
		if (cc != null) 
			cc.registerVariable(nvi);
		return nvi;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		VariableInstance ovi = (VariableInstance) other;
		return variableScope.equals(ovi.variableScope) && variableName.equals(ovi.variableName);
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(variableName.hashCode(),variableScope.hashCode());
	}
}

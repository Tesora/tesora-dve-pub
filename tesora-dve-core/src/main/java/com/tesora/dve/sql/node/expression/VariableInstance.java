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

import com.tesora.dve.exceptions.PEMappedException;
import com.tesora.dve.variables.VariableService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.variables.AbstractVariableAccessor;
import com.tesora.dve.variables.UserVariableAccessor;
import com.tesora.dve.variables.VariableAccessor;
import com.tesora.dve.variables.VariableManager;

public class VariableInstance extends ExpressionNode {

	protected UnqualifiedName variableName;
	protected VariableScope variableScope;
	// for session, global variables - indicate whether we must use the '@@global.<name>' form vs 'global <name>' form
	protected boolean rhsForm;
		
	public VariableInstance(UnqualifiedName varName, VariableScope scope, SourceLocation sloc, boolean rhs) {
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
	
	public boolean getRHSForm() {
		return rhsForm;
	}
	
	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		return new NameAlias(new UnqualifiedName("var_"));
	}

	public AbstractVariableAccessor buildAccessor(SchemaContext sc) {
		if (getScope().getKind() == VariableScopeKind.USER)
			return new UserVariableAccessor(variableName.get());
		else try {
			VariableManager vm = Singletons.require(VariableService.class).getVariableManager();
			return new VariableAccessor(vm.lookupMustExist(sc.getConnection().getVariableSource(),variableName.get()),
					getScope());
		} catch (PEMappedException pe) {
			// these are mapped now
			throw new SchemaException(pe.getErrorInfo());
		} catch (Throwable t) {
			throw new SchemaException(Pass.PLANNER, t);
		}
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		VariableInstance nvi = new VariableInstance(variableName, variableScope, getSourceLocation(), rhsForm);
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

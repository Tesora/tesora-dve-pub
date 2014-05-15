// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

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

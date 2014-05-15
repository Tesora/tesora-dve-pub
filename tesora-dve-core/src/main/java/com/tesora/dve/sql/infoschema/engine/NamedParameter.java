// OS_STATUS: public
package com.tesora.dve.sql.infoschema.engine;


import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

// a hibernate style named parameter (i.e. :foo)
public class NamedParameter extends ExpressionNode {

	protected Name parameterName;
	
	public NamedParameter(Name name) {
		super((SourceLocation)null);
		parameterName = name;
	}
	
	public Name getName() {
		return parameterName;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new NamedParameter(parameterName);
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		NamedParameter onp = (NamedParameter) other;
		return parameterName.equals(onp.parameterName);
	}

	@Override
	protected int selfHashCode() {
		return parameterName.hashCode();
	}

}

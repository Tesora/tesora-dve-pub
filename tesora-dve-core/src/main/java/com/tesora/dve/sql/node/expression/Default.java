// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.transform.CopyContext;

// i.e. the use of the word default in an insert tuple
public class Default extends ExpressionNode {

	public Default(SourceLocation sloc) {
		super(sloc);
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new Default(getSourceLocation());
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return true;
	}

	@Override
	protected int selfHashCode() {
		return 0;
	}

}

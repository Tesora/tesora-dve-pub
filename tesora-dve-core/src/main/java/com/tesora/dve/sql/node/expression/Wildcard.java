// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.transform.CopyContext;

public class Wildcard extends ExpressionNode {

	public Wildcard(SourceLocation sloc) {
		super(sloc);
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new Wildcard(getSourceLocation());
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

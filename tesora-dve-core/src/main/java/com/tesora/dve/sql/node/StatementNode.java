// OS_STATUS: public
package com.tesora.dve.sql.node;

import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.transform.CopyContext;

public abstract class StatementNode extends LanguageNode {

	protected StatementNode(SourceLocation sloc) {
		super(sloc);
	}
	
	@Override
	public LanguageNode copy(CopyContext in) {
		throw new MigrationException("No copy support for " + this.getClass().getName());
	}

}

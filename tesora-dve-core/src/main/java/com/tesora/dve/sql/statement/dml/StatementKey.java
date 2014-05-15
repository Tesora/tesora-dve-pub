// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.node.LanguageNode;

public class StatementKey extends RewriteKey {

	private final DMLStatement stmt;
	
	public StatementKey(DMLStatement dmls) {
		super();
		this.stmt = dmls;
	}
	
	@Override
	protected int computeHashCode() {
		return stmt.getSchemaHashCode();
	}

	@Override
	public LanguageNode toInstance() {
		return stmt;
	}

}

// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import java.util.List;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;

public class MutatorState {

	protected SelectStatement original;
	protected SelectStatement statement;
	
	public MutatorState(SelectStatement orig) {
		original = orig;
		statement = original;
	}
	
	public SelectStatement getStatement() {
		return statement;
	}
	
	public void combine(SchemaContext sc, List<ExpressionNode> projection, boolean makeCopy) {
		if (makeCopy)
			statement = CopyVisitor.copy(original);
		statement.setProjection(projection);
		statement.normalize(sc);
	}
	

}

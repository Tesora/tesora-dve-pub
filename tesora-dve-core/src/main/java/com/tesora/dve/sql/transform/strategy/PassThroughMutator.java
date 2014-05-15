// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import java.util.List;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;

public class PassThroughMutator extends ColumnMutator {

	@Override
	public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ignored) {
		return getSingleColumn(proj, getBeforeOffset());
	}

	@Override
	public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption ignored) {
		return getSingleColumn(proj, getAfterOffsetBegin());
	}

	
}

// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;

class OriginalProjectionBuffer extends Buffer {

	public OriginalProjectionBuffer() {
		super(BufferKind.OP,null);
	}
	
	@Override
	public void adapt(SchemaContext sc, SelectStatement stmt) {
		for(ExpressionNode en : stmt.getProjection())
			add(new BufferEntry(ExpressionUtils.getTarget(en)));
	}

}
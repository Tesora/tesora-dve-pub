// OS_STATUS: public
package com.tesora.dve.sql.expression;

import com.tesora.dve.sql.node.expression.ExpressionNode;

public class ExpressionKey extends RewriteKey {

	protected ExpressionNode embedded;
	
	public ExpressionKey(ExpressionNode em) {
		super();
		embedded = em;
	}
	
	public ExpressionNode getExpression() {
		return embedded;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (obj instanceof ExpressionKey) {
			// use schema equals - rewrite keys are about schema equality, not object equality
			ExpressionKey ek = (ExpressionKey) obj;
			return ek.embedded.isSchemaEqual(embedded);
		}
		return false;
	}

	@Override
	public String toString() {
		return "ExpressionKey{" + embedded + "}";
	}

	@Override
	public ExpressionNode toInstance() {
		return embedded;
	}

	@Override
	protected int computeHashCode() {
		return embedded.getSchemaHashCode();
	}
	
}

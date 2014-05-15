// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import com.tesora.dve.sql.node.expression.ExpressionNode;

public class RedistBufferEntry extends BufferEntry {

	protected ExpressionNode mapped;
	
	public RedistBufferEntry(ExpressionNode targ) {
		super(targ);
	}
	
	public boolean isRedistRequired() {
		return true;
	}

	public void setMapped(ExpressionNode m) {
		mapped = m;
	}
	
	public ExpressionNode getMapped() {
		return mapped;
	}	
}

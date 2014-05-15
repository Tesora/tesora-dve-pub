// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;

public class SubstitutingBufferEntry extends BufferEntry {

	List<ExpressionNode> repl;
	
	public SubstitutingBufferEntry(ExpressionNode targ, List<ColumnInstance> replacementExprs) {
		super(targ);
		repl = new ArrayList<ExpressionNode>();
		for(ColumnInstance ci : replacementExprs)
			repl.add(ci);
	}

	@Override
	public List<ExpressionNode> getNext() {
		return repl;
	}

	@Override
	public ExpressionNode buildNew(List<ExpressionNode> intermediateProjection) {
		return getTarget();
	}
	
}

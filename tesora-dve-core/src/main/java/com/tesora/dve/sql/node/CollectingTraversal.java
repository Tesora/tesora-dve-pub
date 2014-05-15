// OS_STATUS: public
package com.tesora.dve.sql.node;

import com.tesora.dve.sql.node.test.NodeTest;

public class CollectingTraversal extends GeneralCollectingTraversal {

	private final NodeTest<LanguageNode> t;
	
	public CollectingTraversal(Order order, NodeTest<LanguageNode> test) {
		super(order,ExecStyle.ONCE);
		t = test;
	}
	
	@Override
	public boolean is(LanguageNode ln) {
		if (t.has(ln))
			return true;
		return false;
	}

}

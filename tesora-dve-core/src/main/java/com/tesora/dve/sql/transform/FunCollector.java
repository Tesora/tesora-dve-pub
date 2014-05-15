// OS_STATUS: public
package com.tesora.dve.sql.transform;

import java.util.Collection;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.util.ListSet;

public class FunCollector extends GeneralCollectingTraversal {

	public FunCollector() {
		super(Order.POSTORDER,ExecStyle.ONCE);
	}
	
	public static ListSet<FunctionCall> collectFuns(LanguageNode in) {
		return GeneralCollectingTraversal.collect(in, new FunCollector());
	}

	public static ListSet<FunctionCall> collectFuns(Edge<?,?> e) {
		return GeneralCollectingTraversal.collect(e, new FunCollector());
	}

	public static ListSet<FunctionCall> collectFuns(Collection<Edge<?,?>> edges) {
		return GeneralCollectingTraversal.collectEdges(edges, new FunCollector());
	}
	
	
	@Override
	public boolean is(LanguageNode ln) {
		return EngineConstant.FUNCTION.has(ln);
	}
	
}
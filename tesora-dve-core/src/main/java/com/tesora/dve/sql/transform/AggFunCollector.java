// OS_STATUS: public
package com.tesora.dve.sql.transform;


import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.util.ListSet;

public final class AggFunCollector extends GeneralCollectingTraversal {

	private AggFunCollector() {
		super(Order.POSTORDER,ExecStyle.ONCE);
	}
	
	public static ListSet<FunctionCall> collectAggFuns(LanguageNode in) {
		return collect(in,new AggFunCollector());
	}
	
	public static ListSet<FunctionCall> collectAggFuns(Edge<?,?> in) {
		return cast(collect(in,new AggFunCollector()));
	}
	
	@Override
	public boolean is(LanguageNode iln) {
		if (EngineConstant.FUNCTION.has(iln)) {
			FunctionCall fc = (FunctionCall) iln;
			if (fc.getFunctionName().isAggregate()) {
				if (fc.getFunctionName().isUnsupportedAggregate())
					throw new SchemaException(Pass.PLANNER, "Unsupported aggregation function: " + fc.getFunctionName().getSQL());
				return true;
			}
		}
		return false;
	}

}

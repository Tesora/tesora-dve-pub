// OS_STATUS: public
package com.tesora.dve.sql.transform;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryPredicate;

public final class NullFunCollector extends GeneralCollectingTraversal {

	private NullFunCollector() {
		super(Order.POSTORDER,ExecStyle.ONCE);
	}
	
	public static ListSet<FunctionCall> collectNullFuns(LanguageNode in) {
		return collect(in,new NullFunCollector());
	}
	
	public static ListSet<FunctionCall> collectNullFuns(Edge<?,?> in) {
		return collect(in, new NullFunCollector());
	}
	
	@Override
	public boolean is(LanguageNode iln) {
		if (EngineConstant.FUNCTION.has(iln)) {
			FunctionCall fc = (FunctionCall) iln;
			return isNullFun.test(fc);
		}
		return false;
	}

	public static final UnaryPredicate<FunctionCall> isNullFun = new UnaryPredicate<FunctionCall>() {

		@Override
		public boolean test(FunctionCall fc) {
			return (fc.getFunctionName().isIs() || fc.getFunctionName().isNotIs() || fc.getFunctionName().isIfNull());
		}
		
	};
	
}

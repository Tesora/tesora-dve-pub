// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.aggregation;

import java.util.List;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.ColumnMutator;
import com.tesora.dve.sql.transform.strategy.MutatorState;
import com.tesora.dve.sql.transform.strategy.PassThroughMutator;
import com.tesora.dve.sql.transform.strategy.ProjectionMutator;
import com.tesora.dve.sql.transform.strategy.RecallingMutator;

public class SimplifyingMutator extends ProjectionMutator {

	public SimplifyingMutator(SchemaContext sc) {
		super(sc);
	}
	
	@Override
	public List<ExpressionNode> adapt(MutatorState ms, List<ExpressionNode> proj) {
		for(int i = 0; i < proj.size(); i++) {
			ExpressionNode targ = ColumnMutator.getProjectionEntry(proj, i);
			ColumnMutator cm = null;
			boolean isAggFun = EngineConstant.AGGFUN.has(targ);
			if (isAggFun) {
				FunctionCall fc = (FunctionCall) targ;
				if (fc.getFunctionName().isCount() && fc.getParametersEdge().get(0) instanceof Wildcard) {
					isAggFun = false;
				}
			}
			if (isAggFun)
				cm = new RecallingMutator();
			else
				cm = new PassThroughMutator();
			cm.setBeforeOffset(i);
			columns.add(cm);
		}
		List<ExpressionNode> out = applyAdapted(proj,ms);
		AggregationMutatorState ams = (AggregationMutatorState) ms;
		ams.clearGroupBys();
		return out;
	}
}

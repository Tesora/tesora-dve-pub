// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.aggregation;

import java.util.List;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.ColumnMutator;
import com.tesora.dve.sql.transform.strategy.CompoundExpressionColumnMutator;
import com.tesora.dve.sql.transform.strategy.MutatorState;
import com.tesora.dve.sql.transform.strategy.PassThroughMutator;
import com.tesora.dve.sql.transform.strategy.ProjectionMutator;

public class CompoundExpressionProjectionMutator extends ProjectionMutator {

	public CompoundExpressionProjectionMutator(SchemaContext sc) {
		super(sc);
	}

	@Override
	public List<ExpressionNode> adapt(MutatorState ms, List<ExpressionNode> proj) {
		ProjectionCharacterization pc = ProjectionCharacterization.getProjectionCharacterization(ms.getStatement());
		List<ColumnCharacterization> ccs = pc.getColumns();
		for(int i = 0; i < ccs.size(); i++) {
			ColumnCharacterization cc = ccs.get(i);
			ColumnMutator cm = null;
			if (!cc.hasAnyAggFuns() || (!cc.hasAnyNonAggFunColumns() && EngineConstant.AGGFUN.has(cc.getEntry().getTarget())))
				cm = new PassThroughMutator();
			else
				cm = new CompoundExpressionColumnMutator();
			cm.setBeforeOffset(i);
			columns.add(cm);
		}
		return applyAdapted(proj,ms);
	}
	
}

package com.tesora.dve.sql.transform.strategy.aggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.ApplyOption;
import com.tesora.dve.sql.transform.strategy.MutatorState;

abstract class AbstractStddevMutator extends AggFunMutator {

	private final Map<AggFunMutator, ExpressionNode> children = new LinkedHashMap<AggFunMutator, ExpressionNode>();

	protected AbstractStddevMutator(final FunctionName fn, final AbstractVarianceMutator varianceMutator) {
		super(fn);
		this.children.put(varianceMutator, null);
	}

	@Override
	public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
		final List<ExpressionNode> out = new ArrayList<ExpressionNode>();
		final FunctionCall fc = (FunctionCall) getProjectionEntry(proj, getBeforeOffset());
		quantifier = fc.getSetQuantifier();

		final ExpressionNode param = fc.getParametersEdge().get(0);
		final ExpressionNode varParam = (ExpressionNode) param.copy(null);
		out.add(varParam);

		return out;
	}

	@Override
	public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts) {
		if (opts.isLastStep()) {
			final ExpressionNode varianceResultExpr = this.children.values().iterator().next();
			return Collections.<ExpressionNode> singletonList(new FunctionCall(FunctionName.makeSqrt(), varianceResultExpr));
		}
		
		final ExpressionNode param = getProjectionEntry(proj, getAfterOffsetBegin());
		return Collections.<ExpressionNode>singletonList(param);
	}

	@Override
	public Map<AggFunMutator, ExpressionNode> getChildren() {
		return this.children;
	}

}
package com.tesora.dve.sql.transform.strategy.aggregation;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.ApplyOption;
import com.tesora.dve.sql.transform.strategy.MutatorState;
import com.tesora.dve.sql.transform.strategy.aggregation.AggFunProjectionMutator.AvgMutator;

abstract class AbstractVarianceMutator extends AggFunMutator {

	private final Map<AggFunMutator, ExpressionNode> children = new LinkedHashMap<AggFunMutator, ExpressionNode>() {
		private static final long serialVersionUID = -4636984875883190733L;

		{
			put(new AvgMutator(), null);
		}
	};
	
	protected static ExpressionNode buildSumOverSites(final ExpressionNode argument, final boolean isSingleSite) {
		if (isSingleSite) {
			argument.setGrouped();
			return argument;
		}
		
		return new FunctionCall(FunctionName.makeSum(), argument);
	}

	protected AbstractVarianceMutator(final FunctionName fn) {
		super(fn);
	}

	@Override
	public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
		final List<ExpressionNode> out = new ArrayList<ExpressionNode>();
		final FunctionCall fc = (FunctionCall) getProjectionEntry(proj, getBeforeOffset());
		quantifier = fc.getSetQuantifier();

		final ExpressionNode param = fc.getParametersEdge().get(0);
		final ExpressionNode countParam = (ExpressionNode) param.copy(null);
		final ExpressionNode varParam = (ExpressionNode) param.copy(null);
		final ExpressionNode dAvg2Param = (ExpressionNode) param.copy(null);
		out.add(countParam);
		out.add(varParam);
		out.add(dAvg2Param);

		return out;
	}

	@Override
	public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts) {
		final ExpressionNode countParam = getProjectionEntry(proj, getAfterOffsetBegin());
		final ExpressionNode varParam = getProjectionEntry(proj, getAfterOffsetBegin() + 1);
		final ExpressionNode dAvg2Param = getProjectionEntry(proj, getAfterOffsetBegin() + 2);
		final ExpressionNode gu = this.children.values().iterator().next();
		
		final FunctionCall n_i = new FunctionCall(FunctionName.makeCount(), countParam);
		n_i.setSetQuantifier(quantifier);

		final FunctionCall s2_i = new FunctionCall(FunctionName.makeVarPop(), varParam);
		s2_i.setSetQuantifier(quantifier);

		final FunctionCall u_i = new FunctionCall(FunctionName.makeAvg(), dAvg2Param);
		final FunctionCall u_i_minus_gu = new FunctionCall(FunctionName.makeMinus(), u_i, gu);
		final FunctionCall dAvg2 = new FunctionCall(FunctionName.makePow(), u_i_minus_gu, LiteralExpression.makeLongLiteral(2));
		dAvg2.setSetQuantifier(quantifier);

		if (opts.getMaxSteps() == 2) {
			// 2.1 - count(x) n_i, var_pop(x) s2_i, pow(avg(x) - gu, 2) dAvg2
			if (opts.getCurrentStep() == 1) {
				final List<ExpressionNode> out = new ArrayList<ExpressionNode>();
				out.add(n_i);
				out.add(s2_i);
				out.add(dAvg2);
				return out;
			}

			// 2.2 - sum(n_i * s2_i) / DOF + sum(n_i * dAvg2) / DOF
			return Collections.singletonList(this.buildVarianceExpression(countParam, varParam, dAvg2Param, false));
		}

		// 1.1 - (count(x) * var_pop(x)) / DOF + (count(x) * pow(avg(x) - gu, 2)) / DOF
		return Collections.singletonList(this.buildVarianceExpression(countParam, varParam, dAvg2Param, true));
	}
	
	private ExpressionNode buildVarianceExpression(final ExpressionNode countParam, final ExpressionNode varParam, final ExpressionNode dAvg2Param, final boolean isSingleSite) {
		final FunctionCall n_i_s2_i = new FunctionCall(FunctionName.makeMult(), countParam, varParam);

		final ExpressionNode sum_n_i_s2_i = AbstractVarianceMutator.buildSumOverSites(n_i_s2_i, isSingleSite);
		final FunctionCall n_i_dAvg2 = new FunctionCall(FunctionName.makeMult(), countParam, dAvg2Param);
		final ExpressionNode sum_n_i_dAvg2 = AbstractVarianceMutator.buildSumOverSites(n_i_dAvg2, isSingleSite);

		final ExpressionNode dof = this.buildDofExpression(countParam, isSingleSite);
		final FunctionCall term_1 = new FunctionCall(FunctionName.makeDivide(), sum_n_i_s2_i, dof);
		final FunctionCall term_2 = new FunctionCall(FunctionName.makeDivide(), sum_n_i_dAvg2, dof);

		final FunctionCall total = new FunctionCall(FunctionName.makePlus(), term_1, term_2);
		total.setGrouped();
		
		return total;
	}

	@Override
	public Map<AggFunMutator, ExpressionNode> getChildren() {
		return this.children;
	}

	/**
	 * Build an expression for DOF (degree-of-freedom) evaluation.
	 * 
	 * No. of DOF for:
	 * - population: N
	 * - sample N - 1 (number of objects minus one for the average)
	 * 
	 * @param n_i Partial object count.
	 * @param isSingleSite TODO
	 */
	protected abstract ExpressionNode buildDofExpression(final ExpressionNode n_i, final boolean isSingleSite);
}
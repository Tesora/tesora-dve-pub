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
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.ApplyOption;
import com.tesora.dve.sql.transform.strategy.MutatorState;

abstract class AbstractStddevMutator extends AggFunMutator {

	private final Map<AggFunMutator, ExpressionNode> children = new LinkedHashMap<AggFunMutator, ExpressionNode>();
	
	private final AbstractVarianceMutator varianceMutator;

	protected AbstractStddevMutator(final FunctionName fn, final AbstractVarianceMutator varianceMutator) {
		super(fn);
		this.varianceMutator = varianceMutator;
	}
	
	/*
	 * We could have the variance as a child, but that would involve an extra redistribution.
	 * We delegate the operations right on the variance instead.
	 */
	
	@Override
	public void setBeforeOffset(int v) {
		this.varianceMutator.setBeforeOffset(v);
	}
	
	@Override
	public void setAfterOffsetBegin(int v) {
		this.varianceMutator.setAfterOffsetBegin(v);
	}
	
	@Override
	public void setAfterOffsetEnd(int v) {
		this.varianceMutator.setAfterOffsetEnd(v);
	}
	
	@Override
	public int getBeforeOffset() {
		return this.varianceMutator.getBeforeOffset();
	}
	
	@Override
	public int getAfterOffsetBegin() {
		return this.varianceMutator.getAfterOffsetBegin();
	}
	
	@Override
	public int getAfterOffsetEnd() {
		return this.varianceMutator.getAfterOffsetEnd();
	}

	@Override
	public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
		return this.varianceMutator.adapt(sc, proj, ms);
	}
	
	@Override
	public boolean containsAfterOffset(int v) {
		return this.varianceMutator.containsAfterOffset(v);
	}

	@Override
	public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts) {
		// Take SQRT of the variance's last step.
		if (opts.isLastStep()) {
			final ExpressionNode varianceResultExpr = this.varianceMutator.apply(proj, opts).get(0);
			return Collections.<ExpressionNode> singletonList(new FunctionCall(FunctionName.makeSqrt(), varianceResultExpr));
		}
		
		return this.varianceMutator.apply(proj, opts);
	}

	@Override
	public Map<AggFunMutator, ExpressionNode> getChildren() {
		return this.varianceMutator.getChildren();
	}

}
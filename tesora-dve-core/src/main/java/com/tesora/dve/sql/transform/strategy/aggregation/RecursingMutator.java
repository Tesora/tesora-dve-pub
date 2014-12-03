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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.ApplyOption;
import com.tesora.dve.sql.transform.strategy.MutatorState;

abstract class RecursingMutator extends AggFunMutator {

	protected RecursingMutator(final FunctionName fn) {
		super(fn);
	}

	@Override
	public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
		FunctionCall fc = (FunctionCall) getProjectionEntry(proj, getBeforeOffset());
		quantifier = fc.getSetQuantifier();
		return Collections.singletonList((ExpressionNode)fc.getParametersEdge().get(0).copy(null));
	}

	@Override
	public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts) {
		ExpressionNode param = getProjectionEntry(proj, getAfterOffsetBegin());
		FunctionCall nfc = new FunctionCall(fn, param);
		nfc.setSetQuantifier(quantifier);
		return Collections.singletonList((ExpressionNode)nfc);
	}
	
}
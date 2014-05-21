// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

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
import java.util.List;

import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;

public class RecallingMutator extends ColumnMutator {

	protected FunctionName was;
	protected SetQuantifier quantifier;
	protected int nargs;
	
	public RecallingMutator() {
		
	}
	
	@Override
	public List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms) {
		ExpressionNode targ = getProjectionEntry(proj,getBeforeOffset());
		FunctionCall fc = (FunctionCall) targ;
		was = fc.getFunctionName();
		quantifier = fc.getSetQuantifier();
		ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
		for(ExpressionNode arg : fc.getParameters()) {
			out.add((ExpressionNode)arg.copy(null));
		}
		nargs = out.size();
		return out;
	}

	@Override
	public List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption ignored) {
		List<ExpressionNode> params = new ArrayList<ExpressionNode>();
		for(int i = 0; i < nargs; i++) 
			params.add(ColumnMutator.getProjectionEntry(proj, getAfterOffsetBegin() + i));
		FunctionCall fc = new FunctionCall(was, params);
		fc.setSetQuantifier(quantifier);
		return Collections.singletonList((ExpressionNode)fc);
	}
}
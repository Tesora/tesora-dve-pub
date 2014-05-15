// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

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
package com.tesora.dve.sql.node.expression;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;

import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.transform.CopyContext;

public class FunctionCall extends ExpressionNode {

	private FunctionName functionName;
	private MultiEdge<FunctionCall, ExpressionNode> parameters =
		new MultiEdge<FunctionCall, ExpressionNode>(FunctionCall.class, this, EdgeName.FUNCTION_PARAMS);
	private SetQuantifier sq;
	
	public FunctionCall(FunctionName funName, List<ExpressionNode> params, SetQuantifier quant, SourceLocation tree) {
		super(tree);
		this.functionName = funName;
		this.parameters.set(params);
		sq = quant;
	}
	
	public FunctionCall(FunctionName funName, List<ExpressionNode> params) {
		this(funName, params, null, null);
	}
	
	public FunctionCall(FunctionName funName, ExpressionNode ...params) {
		this(funName, Arrays.asList(params), null, null);
	}
	
	public boolean isOperator() { return functionName.isOperator(); }
	public FunctionName getFunctionName() { return functionName; }
	public List<ExpressionNode> getParameters() { return parameters.getMulti(); } 

	public MultiEdge<FunctionCall, ExpressionNode> getParametersEdge() {
		return parameters;
	}
	
	public SetQuantifier getSetQuantifier() { return sq; }
	public void setSetQuantifier(SetQuantifier setq) { sq = setq; }
	
	public List<ExpressionNode> getParameters(int startAt) {
		return parameters.getMulti().subList(startAt, parameters.getMulti().size());
	}
	
	public List<ExpressionNode> getNot(ExpressionNode theOne) {
		ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
		for(ExpressionNode e : parameters.getMulti())
			if (e != theOne)
				out.add(e);
		return out;
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ArrayList<ExpressionNode> np = new ArrayList<ExpressionNode>();
		for(ExpressionNode p : getParameters()) {
			np.add((ExpressionNode)p.copy(cc));
		}
		FunctionCall nfc = new FunctionCall(functionName, np);
		nfc.setSetQuantifier(getSetQuantifier());
		return nfc;
	}

	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		return new NameAlias(new UnqualifiedName("func"));
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		FunctionCall fc = (FunctionCall) other;
		if (!functionName.equals(fc.functionName))
			return false;
		if (!ObjectUtils.equals(sq,fc.sq))
			return false;
		return true;
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(functionName.hashCode(),(sq == null ? 0 : sq.hashCode()));
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return (List<T>) Collections.singletonList(parameters);
	}

	@Override
	public List<ExpressionNode> getSubExpressions() {
		return getParameters();
	}
	
	public boolean isUnaryFunction() {
		return (parameters.size() == 1);
	}

}

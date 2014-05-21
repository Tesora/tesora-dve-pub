// OS_STATUS: public
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
import java.util.List;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.transform.CopyContext;

public class WhenClause extends ExpressionNode {

	protected SingleEdge<WhenClause, ExpressionNode> testExpression =
		new SingleEdge<WhenClause, ExpressionNode>(WhenClause.class, this, EdgeName.WHEN_TEST);
	protected SingleEdge<WhenClause, ExpressionNode> resultExpression =
		new SingleEdge<WhenClause, ExpressionNode>(WhenClause.class, this, EdgeName.WHEN_RESULT);
	@SuppressWarnings("rawtypes")
	protected List edges = new ArrayList();

	@SuppressWarnings("unchecked")
	public WhenClause(ExpressionNode te, ExpressionNode re, SourceLocation orig) {
		super(orig);
		setTestExpression(te);
		setResultExpression(re);
		edges.add(testExpression);
		edges.add(resultExpression);
	}

	public void setTestExpression(ExpressionNode en) { testExpression.set(en); }
	public ExpressionNode getTestExpression() { return testExpression.get(); }
	
	public ExpressionNode getResultExpression() { return resultExpression.get(); }
	public void setResultExpression(ExpressionNode ne) { resultExpression.set(ne); }


	@Override
	protected LanguageNode copySelf(CopyContext in) {
		return new WhenClause((ExpressionNode)testExpression.get().copy(in), (ExpressionNode)resultExpression.get().copy(in), getSourceLocation());
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}

	@Override
	public List<ExpressionNode> getSubExpressions() {
		ArrayList<ExpressionNode> subs = new ArrayList<ExpressionNode>();
		subs.add(testExpression.get());
		subs.add(resultExpression.get());
		return subs;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return true;
	}

	@Override
	protected int selfHashCode() {
		return 0;
	}
	
}
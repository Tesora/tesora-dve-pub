package com.tesora.dve.sql.statement.dml.compound;

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
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;

public class StatementWhenClause extends CompoundStatement {

	protected SingleEdge<StatementWhenClause, ExpressionNode> testExpression =
			new SingleEdge<StatementWhenClause, ExpressionNode>(StatementWhenClause.class, this, EdgeName.STMT_WHEN_TEST);
	protected SingleEdge<StatementWhenClause, Statement> resultStatement =
			new SingleEdge<StatementWhenClause, Statement>(StatementWhenClause.class, this, EdgeName.STMT_WHEN_RESULT);
	@SuppressWarnings("rawtypes")
	protected List edges = new ArrayList();

	@SuppressWarnings("unchecked")
	public StatementWhenClause(ExpressionNode te, Statement result, SourceLocation orig) {
		super(orig);
		this.testExpression.set(te);
		this.resultStatement.set(result);
		edges.add(testExpression);
		edges.add(resultStatement);
	}

	public ExpressionNode getTestExpression() {
		return testExpression.get();
	}
	
	public Statement getResultStatement() {
		return resultStatement.get();
	}
	
	
	@Override
	public void normalize(SchemaContext sc) {
		// TODO Auto-generated method stub

	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected int selfHashCode() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}

	
}

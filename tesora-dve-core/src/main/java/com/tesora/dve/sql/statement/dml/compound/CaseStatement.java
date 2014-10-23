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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class CaseStatement extends CompoundStatement {

	protected SingleEdge<CaseStatement, ExpressionNode> testExpression =
			new SingleEdge<CaseStatement, ExpressionNode>(CaseStatement.class, this, EdgeName.CASE_STMT_TESTCLAUSE);
	protected SingleEdge<CaseStatement, Statement> elseExpression =
			new SingleEdge<CaseStatement, Statement>(CaseStatement.class, this, EdgeName.CASE_STMT_ELSECLAUSE);
	protected MultiEdge<CaseStatement, StatementWhenClause> whenClauses =
			new MultiEdge<CaseStatement, StatementWhenClause>(CaseStatement.class, this, EdgeName.CASE_STMT_WHENCLAUSE);
	@SuppressWarnings("rawtypes")
	protected List edges = new ArrayList();	
		
	public CaseStatement(SourceLocation location, ExpressionNode testExpression, 
			List<StatementWhenClause> whenClauses,
			Statement elseExpression) {
		super(location);
		this.testExpression.set(testExpression);
		this.elseExpression.set(elseExpression);
		this.whenClauses.set(whenClauses);
		edges.add(testExpression);
		edges.add(whenClauses);
		edges.add(elseExpression);
	}

	public ExpressionNode getTestExpression() {
		return testExpression.get();
	}
	
	public Statement getElseResult() {
		return elseExpression.get();
	}
	
	public List<StatementWhenClause> getWhenClauses(){
		return whenClauses.getMulti();
	}
	
	public MultiEdge<?,StatementWhenClause> getWhenClausesEdge() {
		return whenClauses;
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

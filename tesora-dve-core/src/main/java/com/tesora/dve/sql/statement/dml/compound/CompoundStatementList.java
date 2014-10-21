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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class CompoundStatementList extends CompoundStatement {

	private final UnqualifiedName label;

	protected MultiEdge<CompoundStatementList, Statement> stmts =
			new MultiEdge<CompoundStatementList, Statement>(CompoundStatementList.class, this, EdgeName.COMPOUND_STATEMENTS);

	public CompoundStatementList(UnqualifiedName label, List<Statement> stmts) {
		super(null);
		this.label = label;
		this.stmts.set(stmts);
	}

	public List<Statement> getStatements() {
		return this.stmts.getMulti();
	}
	
	public MultiEdge<?,Statement> getStatementsEdge() {
		return this.stmts;
	}
	
	
	@Override
	public void normalize(SchemaContext sc) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es,
			BehaviorConfiguration config) throws PEException {
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
		return (List<T>) Collections.singletonList(stmts);
	}
}

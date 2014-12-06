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
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.MultiFeatureStep;

public class CompoundStatementList extends CompoundStatement implements FeaturePlanner {

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

	@Override
	public FeatureStep plan(SchemaContext sc, BehaviorConfiguration config)
			throws PEException {
		MultiFeatureStep out = new MultiFeatureStep(this) {
			
		};
		// each stmt should get it's own ValueManager, which should be ok as we specified 
		// actual literals
		for(Statement s : stmts) {
			if (s.isCompound()) {
				CompoundStatement cs = (CompoundStatement) s;
				out.addChild(cs.plan(sc, config));
			} else if (s.isDML()) {
				DMLStatement dmls = (DMLStatement) s;
				out.addChild(dmls.plan(sc,config));
			} else if (s.isDDL()) {
				throw new PEException("No support for DDL in compound statements");
			}
		}
		// the multifeature step does not have invariants to check
		// however we will still traverse the children
		out.withDefangInvariants();
		return out;
	}

	@Override
	public boolean emitting() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void emit(String what) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context)
			throws PEException {
		throw new PEException("Illegal call to CompoundStatement.plan");
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		// TODO Auto-generated method stub
		return null;
	}
}

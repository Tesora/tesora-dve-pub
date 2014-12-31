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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.CaseExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.WhenClause;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.HasPlanning;
import com.tesora.dve.sql.transform.execution.TriggerBranchExecutionStep;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.MultiFeatureStep;

public class CaseStatement extends CompoundStatement implements FeaturePlanner {

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
		edges.add(this.testExpression);
		edges.add(this.whenClauses);
		edges.add(this.elseExpression);
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

	/**
	 * We need to select the code path to follow. Here we rewrite the original
	 * CASE statement as a CaseExpression which evaluates the CASE condition and
	 * returns the index of the evaluated branch.
	 * We then use this index to choose the appropriate statement to execute.
	 * Branches are indexed starting from 1. The else branch has index 0.
	 */
	@Override
	public FeatureStep plan(final SchemaContext sc, final BehaviorConfiguration config)
			throws PEException {
		final int numCases = this.whenClauses.size() + 1;
		final SelectStatement caseEvaluationStmt = new SelectStatement(new AliasInformation());

		final List<Statement> branchStmts = new ArrayList<Statement>(numCases);
		final List<WhenClause> whens = new ArrayList<WhenClause>(numCases);
		branchStmts.add(this.elseExpression.get()); // ELSE branch must be at index 0.

		// Now append the WHEN branches indexing from 1.
		long branchCounter = 1;
		for (final StatementWhenClause when : this.whenClauses) {
			whens.add(buildLiteralWhenClause(when.getTestExpression(), branchCounter++));
			branchStmts.add(when.getResultStatement());
		}
		
		caseEvaluationStmt.setProjection(Collections.<ExpressionNode> singletonList(new CaseExpression(this.testExpression.get(), LiteralExpression
				.makeLongLiteral(0), whens,
				null)));

		final FeatureStep plan = new MultiFeatureStep(this) {

			/**
			 * CASE evaluation statement is the first child followed by
			 * statements for individual branches.
			 */
			@Override
			public void schedule(PlannerContext pc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
				if (!scheduled.add(this)) {
					return;
				}
				schedulePrefix(pc, es, scheduled);

				final ExecutionPlan parentPlan = es.getPlan();
				final HasPlanning caseEvaluationStep = ExecutionSequence.buildSubSequence(pc, getSelfChildren().get(0), parentPlan); // CASE branch evaluation
				final List<HasPlanning> branchOperationSteps = new ArrayList<HasPlanning>(numCases);
				final int numChildren = getSelfChildren().size();
				for (int i = 1; i < numChildren; i++) {
					branchOperationSteps.add(ExecutionSequence.buildSubSequence(pc, getSelfChildren().get(i), parentPlan));
				}

				es.append(new TriggerBranchExecutionStep(getDatabase(pc), getStorageGroup(sc),
						caseEvaluationStep, branchOperationSteps));

			}
		}.withDefangInvariants();

		plan.addChild(caseEvaluationStmt.plan(sc, config));
		for (final Statement branchStmt : branchStmts) {
			plan.addChild(branchStmt.plan(sc, config));
		}

		return plan;
	}

	private WhenClause buildLiteralWhenClause(final ExpressionNode branchTest, final long branchIndex) {
		return new WhenClause(branchTest, LiteralExpression.makeLongLiteral(branchIndex), null);
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
	public FeatureStep plan(DMLStatement stmt, PlannerContext context) throws PEException {
		throw new PEException("Illegal call to CompoundStatement.plan");
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		// TODO Auto-generated method stub
		return null;
	}

}

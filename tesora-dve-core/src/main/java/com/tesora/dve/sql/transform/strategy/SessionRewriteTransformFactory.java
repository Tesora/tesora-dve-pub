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


import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang.StringEscapeUtils;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ActualLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.RandFunctionCall;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.node.test.EngineToken;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeatureStepBuilder;
import com.tesora.dve.sql.transform.execution.AdhocResultsSessionStep;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.NonDMLFeatureStep;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.variables.AbstractVariableAccessor;

/*
 * Applies when variables are present in the query.  If found, we sub in the current values of
 * them and plan as normal.  If there are not persistent tables in the query, and there are no functions
 * we may attempt to build the uresult set ourselves.
 * 
 * This transform also handles the rand function.
 */
public class SessionRewriteTransformFactory extends TransformFactory {

	private static boolean hasFunctionCall(final DMLStatement stmt, final EngineToken function, final SchemaContext sc) {
		final ListSet<FunctionCall> functionCalls = EngineConstant.FUNCTIONS.getValue(stmt, sc);
		for (final FunctionCall call : functionCalls) {
			if (EngineConstant.FUNCTION.has(call, function)) {
				return true;
			}
		}

		return false;
	}

	private boolean applies(SchemaContext sc, DMLStatement stmt) throws PEException {
		return EngineConstant.VARIABLES.hasValue(stmt,sc) 
				|| !EngineConstant.TABLES_INC_NESTED.hasValue(stmt, sc)
				|| hasFunctionCall(stmt, EngineConstant.RAND, sc);
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.SESSION;
	}

	private static final DMLExplainRecord tooComplexExplain = DMLExplainReason.SESSION_TOO_COMPLEX.makeRecord();

	private void rewriteUserDefinedVariables(final DMLStatement stmt, final SchemaContext sc) throws PEException {
		final ListSet<VariableInstance> vis = EngineConstant.VARIABLES.getValue(stmt, sc);
		for (final VariableInstance vi : vis) {
			if (vi.getScope().isUserScope() && DBNative.DVE_SITENAME_VAR.equals(vi.getVariableName().get().toLowerCase()))
				continue;
			final Edge<?, ExpressionNode> parentEdge = vi.getParentEdge();
			final AbstractVariableAccessor va = vi.buildAccessor();
			parentEdge.set(LiteralExpression.makeStringLiteral(PEStringUtils.dequote(sc.getConnection().getVariableValue(va))));
		}
	}

	private void rewriteFunctionCalls(final DMLStatement stmt, final SchemaContext sc) {
		final ListSet<FunctionCall> functionCalls = EngineConstant.FUNCTIONS.getValue(stmt, sc);
		for (final FunctionCall call : functionCalls) {
			if (EngineConstant.FUNCTION.has(call, EngineConstant.RAND)) {
				rewriteRandFunctionCall((RandFunctionCall) call);
			}
		}
	}

	private void rewriteRandFunctionCall(final RandFunctionCall rand) {
		if (rand.hasSeed()) {
			rand.setSeed(new ActualLiteralExpression(rand.getSeed(), TokenTypes.Signed_Integer, rand.getSeed().getSourceLocation(), null));
		} else {
			rand.setSeed(LiteralExpression.makeSignedIntegerLiteral(ThreadLocalRandom.current().nextInt()));
		}
	}

	private static class TooComplexTraversal extends Traversal {

		private boolean complex = false;
		
		public TooComplexTraversal() {
			super(Order.PREORDER, ExecStyle.ONCE);
		}
		
		public boolean isComplex() {
			return complex;
		}
		
		public boolean allow(Edge<?,?> e) {
			return !complex;
		}
		
		public boolean allow(LanguageNode ln) {
			return !complex;
		}

		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof LiteralExpression 
					|| in instanceof SelectStatement 
					|| EngineConstant.VARIABLE.has(in) 
					|| in instanceof ExpressionAlias
					|| in instanceof LimitSpecification) {
				// nothing
			} else {
				complex = true;
			}
			return in;
		}
		
		
	}

	@Override
	public FeatureStep plan(final DMLStatement stmt, PlannerContext incontext) throws PEException {
		if (!applies(incontext.getContext(), stmt))
			return null;
		PlannerContext context = incontext.withTransform(getFeaturePlannerID());
		DMLStatement copy = CopyVisitor.copy(stmt);
		// swap in current values of all variables
		rewriteUserDefinedVariables(copy, context.getContext());
		rewriteFunctionCalls(copy, context.getContext());

		// complex really means - it's not something like select 1, select @@version_comment -
		// essentially if we encounter anything that's not a literal or a var, (excluding expr aliases), we have to push down.
		TooComplexTraversal tca = new TooComplexTraversal();
		tca.traverse(copy);

		
		if (EngineConstant.TABLES_INC_NESTED.hasValue(copy,context.getContext())) {
			return buildPlan(copy, context, DefaultFeaturePlannerFilter.INSTANCE);
		}
		FeatureStep root = null;

		if (tca.isComplex() || incontext.getContext().getOptions().isForceSessionPushdown()) {
			// we must push it down as a regular statement
			if (copy instanceof ProjectingStatement) {
				root = DefaultFeatureStepBuilder.INSTANCE.buildProjectingStep(context,
						this,
						(ProjectingStatement)copy, 
						new ExecutionCost(false,false,null,-1), 
						context.getContext().getSessionStatementStorageGroup(), 
						context.getContext().getCurrentPEDatabase(false),
						new DistributionVector(incontext.getContext(), Collections.<PEColumn> emptyList(), Model.BROADCAST),
						null,
						tooComplexExplain);
			} else {
				root = DefaultFeatureStepBuilder.INSTANCE.buildStep(context,
						this,
						copy, 
						null, 
						tooComplexExplain);
			}
		} else {
			// not complex, no children - so we will fulfill this inline
			final SelectStatement fcopy = (SelectStatement) copy;
			root = new NonDMLFeatureStep(this, context.getContext().getSessionStatementStorageGroup()) {

				@Override
				public void scheduleSelf(PlannerContext pc, ExecutionSequence es)
						throws PEException {
					ProjectionInfo projInfo = stmt.getProjectionMetadata(pc.getContext());
					ColumnSet md = new ColumnSet();
					ResultRow row = new ResultRow();
					for(int i = 0; i < fcopy.getProjectionEdge().size(); i++) {
						ExpressionNode en = fcopy.getProjectionEdge().get(i);
						if (en instanceof ExpressionAlias)
							en = ((ExpressionAlias)en).getTarget();
						LiteralExpression litex = (LiteralExpression) en;
						md.addColumn(projInfo.getColumnInfo(i + 1).getName(), 255, "varchar", java.sql.Types.VARCHAR);
						ColumnMetadata cmd = md.getColumn(i+1);
						cmd.setAliasName(projInfo.getColumnInfo(i+1).getAlias());
						row.addResultColumn(litex.isStringLiteral() 
								? StringEscapeUtils.unescapeJava((String) litex.getValue(pc.getContext())) 
										: litex.getValue(pc.getContext()));
					}
					es.append(new AdhocResultsSessionStep(new IntermediateResultSet(md,row)));
				}					
			};
		}

		return root.withCachingFlag(false);
	}
}

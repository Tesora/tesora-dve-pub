package com.tesora.dve.sql.statement.session;

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

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.errmap.AvailableErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepFilterOperation.OperationFilter;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LateResolvingVariableExpression;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.statement.CacheableStatement;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.FilterExecutionStep;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.transform.execution.SetVariableExecutionStep;
import com.tesora.dve.sql.transform.execution.SetVariableExecutionStep.VariableValueSource;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.variable.VariableConstants;
import com.tesora.dve.variables.AbstractVariableAccessor;
import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.variables.VariableManager;


public class SessionSetVariableStatement extends SessionStatement implements CacheableStatement {
	
	private List<SetExpression> exprs;
	private ListSet<TableKey> empty;
	
	public SessionSetVariableStatement(List<SetExpression> exprs) {
		super();
		this.exprs = exprs;
		this.empty = new ListSet<TableKey>();
	}
	
	public List<SetExpression> getSetExpressions() {
		return this.exprs;
	}
	
	@Override
	public void plan(SchemaContext pc,ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		normalize(pc);
		// we have 7 cases:
		// [1] a txn isolation set - this just gets pushed down
		// [2] set a known variable to a constant - route to qsos
		// [3] set a known variable to a user var - look up the user var and then treat like [2]
		// [4] set a user var to a known variable - look up the known variable and then set the user var
		// [5] set a user var to a constant - set the user var
		// [6] set either a known variable or a user var to an expression - throw an exception
		// [7] set names - turn into the 3 variables
		
		// so we're going to handle cases 1 and 7 separately
		// for 2 - 5, schedule using the QSOsetScoped, either with the constant value or with an accessor
		// that looks up the right thing
		// this also means we could schedule a complex multi set as a bunch of qsos, but that's ok
		// this also means we no longer need the empty exec step - we'll always do something
		
		if (es.getPlan() != null)
			es.getPlan().setCacheable(true);
		
		List<SetExpression> sets = new ArrayList<SetExpression>(exprs);
		exprs.clear();
		
		VariableManager vm = Singletons.require(HostService.class).getVariableManager();
		
		for(SetExpression se : sets) {
			if (se.getKind() == SetExpression.Kind.TRANSACTION_ISOLATION) {
				SetTransactionIsolationExpression stie = (SetTransactionIsolationExpression) se;
				assertPrivilege(pc,vm.lookupMustExist(pc.getConnection().getVariableSource(),
						VariableConstants.TRANSACTION_ISOLATION_LEVEL_NAME),stie.getScope());
				handleSetTransactionIsolation(pc, stie,es);
			} else {
				SetVariableExpression sve = (SetVariableExpression) se;
				VariableInstance vi = sve.getVariable();
				final String variableName = vi.getVariableName().get();
				if (variableName.equalsIgnoreCase("names")) {
					handleSetNames(pc,sve,es);
				} else if (variableName.equalsIgnoreCase("sql_safe_updates")) {
					// don't pass this down just eat the command
					es.append(new EmptyExecutionStep(0, "set sql_safe_updates"));
				} else {
					if (sve.getValue().size() > 1) {
						throw new PEException("Illegal set expression, multiple values");
					}
					if (vi.getScope().getKind() != VariableScopeKind.USER && vi.getScope().getKind() != VariableScopeKind.SCOPED) {
						assertPrivilege(pc,vm.lookupMustExist(pc.getConnection().getVariableSource(),
								vi.getVariableName().getUnquotedName().get()),vi.getScope());
					}

					if (variableName.equalsIgnoreCase(VariableConstants.SLOW_QUERY_LOG_NAME)
							|| variableName.equalsIgnoreCase(VariableConstants.LONG_QUERY_TIME_NAME)) {
						handleSetGlobalVariableExpression(pc,sve, es);
					} else {
						handleSetVariableExpression(pc, sve, vm, es);
					}
				}
			}
		}
		exprs = sets;
	}
	
	private void handleSetNames(SchemaContext pc, SetVariableExpression sve, ExecutionSequence es) throws PEException {
		// just names for right now - anything else we should hurl on
		List<ExpressionNode> value = sve.getValue();
		// the first arg is the charset, and the second is the collation
		VariableValueSource nva = getRHSSource(pc,value.get(0));
		String[] mapsTo = new String[] { "character_set_client", "character_set_connection", "character_set_results" };
		for(String vn : mapsTo) {
			// do we need to set the persistent group here?
			es.append(new SetVariableExecutionStep(new VariableScope(VariableScopeKind.SESSION), vn, nva,pc.getPersistentGroup()));
		}
		if (value.size() == 2) {
			VariableValueSource cva = getRHSSource(pc,value.get(1));
			// do we need to set the persistent group here?
			es.append(new SetVariableExecutionStep(new VariableScope(VariableScopeKind.SESSION),"collation_connection",cva, pc.getPersistentGroup()));
		}
		if (value.size() > 2)
			throw new PEException("Too many parameters to set names");
	}
	
	private VariableValueSource getRHSSource(SchemaContext pc, ExpressionNode rhs) throws PEException {
		if (EngineConstant.CONSTANT.has(rhs)) {
			LiteralExpression litex = (LiteralExpression) rhs;
			return SetVariableExecutionStep.makeSource(litex);
		} else if (EngineConstant.VARIABLE.has(rhs)) {
			VariableInstance rvi = (VariableInstance) rhs;
			final AbstractVariableAccessor va = rvi.buildAccessor(pc);
			return SetVariableExecutionStep.makeSource(va);
		}
		return null;
	}
	
	private void handleSetVariableExpression(SchemaContext pc, SetVariableExpression sve, VariableManager vm, ExecutionSequence es) throws PEException {
		VariableInstance vi = sve.getVariable();
		List<ExpressionNode> value = sve.getValue();
		// figure out the scope
		ExpressionNode rhs = value.get(0);
		VariableValueSource nva = getRHSSource(pc,rhs);
		if (nva != null) {
			final String variableName = vi.getVariableName().get();
			if (nva.isConstant()) {
				final VariableHandler<?> vh = vm.lookup(variableName);
				if ((vh != null) && vh.getMetadata().isNumeric()
						&& PEStringUtils.isQuoted(String.valueOf(sve.getVariableExpr()))) {
					throw new SchemaException(new ErrorInfo(AvailableErrors.WRONG_TYPE_FOR_VARIABLE, variableName));
				}
			}
			es.append(new SetVariableExecutionStep(vi.getScope(), variableName, nva, pc.getPersistentGroup()));
		} else {
			// the rhs is complex - we need to execute it on a p.site or a dyn site (most likely a p.site)
			handleComplexSetVariableExpression(pc,vi, rhs, es);
		}
	}

	private void handleSetTransactionIsolation(SchemaContext pc, SetTransactionIsolationExpression stie, ExecutionSequence es) throws PEException {
		VariableValueSource nva = SetVariableExecutionStep.makeSource(stie.getLevel().getHostSQL());
		es.append(new SetVariableExecutionStep(stie.getScope(),VariableConstants.TRANSACTION_ISOLATION_LEVEL_NAME,nva, pc.getPersistentGroup()));
	}

	private void assertPrivilege(SchemaContext pc, VariableHandler handler, VariableScope requestedScope) throws PEException {
		PEUser currentUser = pc.getCurrentUser().get(pc);
		if (currentUser.isRoot()) return; // root has all privileges
		// we must be root if the requested scope is persistent or if the target handler contains a global scope & we are requesting one
		if (requestedScope.getKind() == VariableScopeKind.PERSISTENT)
			throw new PEException("Must be root to set persistent value of " + handler.getName());
		if (requestedScope.getKind() == VariableScopeKind.GLOBAL && handler.getScopes().contains(VariableScopeKind.GLOBAL)) 
			throw new PEException("Must be root to set global value of " + handler.getName());
	}
	
	@Override
	public void normalize(SchemaContext pc) {
		// should do some sort of type checking here if we know the variable type
		for(SetExpression se : exprs) {
			if (se.getKind() == SetExpression.Kind.VARIABLE) {
				SetVariableExpression sve = (SetVariableExpression) se;
				if (sve.getVariable().getScope().getKind() == VariableScopeKind.SESSION) {
					String vn = sve.getVariable().getVariableName().get();
					if (VariableConstants.SQL_AUTO_IS_NULL_NAME.equals(vn.toLowerCase())) {
						// only catch the literal variety for now
						ExpressionNode value = sve.getValue().get(0);
						if (value instanceof LiteralExpression) {
							LiteralExpression litex = (LiteralExpression) value;
							String strval = litex.getValue(pc).toString();
							if ("1".equals(strval.trim())) {
								throw new SchemaException(Pass.PLANNER, "No support for " + VariableConstants.SQL_AUTO_IS_NULL_NAME + " = 1 (planned)");
							}
						}
					}					
				}
			}
		}
	}
	
	private void handleComplexSetVariableExpression(SchemaContext pc, VariableInstance vi,
			ExpressionNode rhs, ExecutionSequence es) throws PEException {
		CopyContext cc = new CopyContext("complex set var");
		ExpressionNode copy = CopyVisitor.copy(rhs,cc);
		new ReplaceVariablesTraversal(pc).traverse(copy);
		SelectStatement ss = new SelectStatement(new AliasInformation());
		ss.setProjection(Collections.singletonList(copy));
		ProjectingExecutionStep adhocStep = 
				ProjectingExecutionStep.build(pc, pc.getCurrentDatabase(), buildOneSiteGroup(pc), null, null, ss, null);
		SetVariableOperationFilter filter = new SetVariableOperationFilter(vi.buildAccessor(pc));
		es.append(new FilterExecutionStep(adhocStep, filter));		
	}
	
	
	private void handleSetGlobalVariableExpression(SchemaContext pc, final SetVariableExpression sve, final ExecutionSequence es)
			throws PEException {
		final VariableInstance vi = sve.getVariable();
		final String variableName = vi.getVariableName().get();
		final VariableValueSource nva = getRHSSource(pc,sve.getValue().get(0));
		es.append(new
				SetVariableExecutionStep(vi.getScope(),
						variableName, nva, pc.getPersistentGroup()));
	}

	private static class SetVariableOperationFilter implements OperationFilter {

		private final AbstractVariableAccessor target;
		
		public SetVariableOperationFilter(AbstractVariableAccessor targ) {
			target = targ;
		}
		
		@Override
		public void filter(SSConnection ssCon, ColumnSet columnSet,	List<ArrayList<String>> rowData, DBResultConsumer results)
				throws Throwable {
			ArrayList<String> row = rowData.get(0);
			target.setValue(ssCon, row.get(0));
		}

		@Override
		public String describe() {
			return "SetVariableOperationFilter(" + target.getSQL() + ")";
		}
	}

	private static class ReplaceVariablesTraversal extends Traversal {

		private final SchemaContext cntxt;
		
		public ReplaceVariablesTraversal(SchemaContext sc) {
			super(Order.POSTORDER,ExecStyle.ONCE);
			this.cntxt = sc;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.VARIABLE.has(in)) {
				VariableInstance vi = (VariableInstance) in;
				return new LateResolvingVariableExpression(vi.buildAccessor(cntxt));
			}
			return in;
		}
	}
	
	@Override
	public ListSet<TableKey> getAllTableKeys() {
		return empty;
	}
}

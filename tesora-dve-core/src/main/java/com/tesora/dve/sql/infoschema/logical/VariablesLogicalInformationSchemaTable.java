package com.tesora.dve.sql.infoschema.logical;

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
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalQuery;
import com.tesora.dve.sql.infoschema.engine.LogicalSchemaQueryEngine;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.PEQueryVariablesStatement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;

public class VariablesLogicalInformationSchemaTable extends LogicalInformationSchemaTable {
	public static final String TABLE_NAME = "variables";
	public static final String SCOPE_COL_NAME = "Scope";
	public static final String NAME_COL_NAME = "Variable_name";
	public static final String VALUE_COL_NAME = "Value";
	
	public static final String GLOBAL_TABLE_NAME = "global_variables";
	public static final String SESSION_TABLE_NAME = "session_variables";
	public static final String SCOPED_TABLE_NAME = "dve_variables";
	
	protected LogicalInformationSchemaColumn scopeColumn = null;
	protected LogicalInformationSchemaColumn nameColumn = null;
	protected LogicalInformationSchemaColumn valueColumn = null;

	private LogicalInformationSchemaColumn[] withScope;
	private LogicalInformationSchemaColumn[] withoutScope;
	
	public VariablesLogicalInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName(TABLE_NAME));
		scopeColumn = new LogicalInformationSchemaColumn(new UnqualifiedName(SCOPE_COL_NAME), BasicType.buildType(java.sql.Types.VARCHAR, 255, dbn));
		nameColumn = new LogicalInformationSchemaColumn(new UnqualifiedName(NAME_COL_NAME), BasicType.buildType(java.sql.Types.VARCHAR, 255, dbn));
		valueColumn = new LogicalInformationSchemaColumn(new UnqualifiedName(VALUE_COL_NAME), BasicType.buildType(java.sql.Types.VARCHAR, 255, dbn));
		scopeColumn.setTable(this);
		nameColumn.setTable(this);
		valueColumn.setTable(this);
		addColumn(null,scopeColumn);
		addColumn(null,nameColumn);
		addColumn(null,valueColumn);
		withScope = new LogicalInformationSchemaColumn[] { scopeColumn, nameColumn, valueColumn };
		withoutScope = new LogicalInformationSchemaColumn[] { nameColumn, valueColumn };
	}
	
	@Override
	public String getTableName() {
		return TABLE_NAME;
	}
	
	// the variables engine.  the way this works is if we can fulfill the query ourselves, we'll return
	// a SchemaQueryStatement with an IntermediateResultSet.  If we can't, then we'll return a 
	// PEQueryVariablesStatement.
	
	public Statement execute(SchemaContext sc, ViewQuery vq, ProjectionInfo pi) {
		// logical only
		LogicalQuery lq = LogicalSchemaQueryEngine.convertDown(sc, vq);
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(lq.getQuery().getWhereClause());
		String scopeName = findScope(sc,decompAnd);
		VariableScope vs = rebuildScope(scopeName);
		// there is basically on case we don't push down:
		// variable_name like ?.
		// therefore, if there is more than one remaining filter, we must push down.  if the one remaining filter doesn't look
		// like a like expression, we also push down.  furthermore, we push down if the projection is not just the columns
		String likeExpr = null;
		if (decompAnd.isEmpty()) {
			likeExpr = "";
		} else if (decompAnd.size() == 1) {
			if (decompAnd.get(0) instanceof FunctionCall) {
				FunctionCall fc = (FunctionCall) decompAnd.get(0);
				if (fc.getFunctionName().isLike()) {
					LiteralExpression likeArg = (LiteralExpression) fc.getParametersEdge().get(1);
					likeExpr = (String) likeArg.getValue(sc);
				}
			}
		}
		if (likeExpr != null) {
			for(ExpressionNode en : lq.getQuery().getProjectionEdge()) {
				ExpressionNode targ = ExpressionUtils.getTarget(en);
				if (!(targ instanceof ColumnInstance)) {
					likeExpr = null;
					break;
				}
			}
		}
		if (likeExpr != null) {
			IntermediateResultSet irs = executeLikeSelect(sc,likeExpr,vs,lq,pi);
			return new SchemaQueryStatement(false,getName().get(),irs);
		} else {
			lq.getQuery().setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
			return new PEQueryVariablesStatement(lq.getQuery(),vs);
		}		
	}
	
	private String findScope(SchemaContext sc, List<ExpressionNode> wcs) {
		for(Iterator<ExpressionNode> iter = wcs.iterator(); iter.hasNext();) {
			ExpressionNode en = iter.next();
			if (en instanceof FunctionCall) {
				FunctionCall fc = (FunctionCall) en;
				if (fc.getFunctionName().isEquals()) {
					ExpressionNode firstArg = fc.getParametersEdge().get(0);
					if (firstArg instanceof ColumnInstance) {
						ColumnInstance ci = (ColumnInstance) firstArg;
						if (ci.getColumn() == scopeColumn) {
							iter.remove();
							LiteralExpression litex = (LiteralExpression) fc.getParametersEdge().get(1);
							return (String)litex.getValue(sc);
						}
					}
				}
			}
		}
		throw new InformationSchemaException("Unable to find scoping filter on variables query");
	}
	
	
	
	private UnaryPredicate<List<String>> buildPredicate(String likeExpr) {
		if (likeExpr == null || "".equals(likeExpr.trim())) {
			return new UnaryPredicate<List<String>>() {

				@Override
				public boolean test(List<String> object) {
					return true;
				}
			
			};
		}
		
		final Pattern p = PEStringUtils.buildSQLPattern(likeExpr);
		return new UnaryPredicate<List<String>>() {

			@Override
			public boolean test(List<String> object) {
				Matcher m = p.matcher(object.get(1));
				return m.matches();
			}
			  
		};
	}

	public IntermediateResultSet executeLikeSelect(SchemaContext sc,
			String likeExpr, VariableScope vs, LogicalQuery lq, ProjectionInfo pi) {
		List<List<String>> vars = null;
		try {
			vars = sc.getConnection().getVariables(vs);
		} catch (PEException e) {
			e.printStackTrace();
			return null;
		}
		List<UnaryFunction<String,List<String>>> rowSelectors = buildRowSelectors(lq);
		ColumnSet cs = LogicalSchemaQueryEngine.buildProjectionMetadata(lq.getProjectionColumns(),pi,null);
		// ColumnSet md = buildColumnSet(showScopeName);
		List<ResultRow> rows = new ArrayList<ResultRow>();
		UnaryPredicate<List<String>> pred = buildPredicate(likeExpr);
		for(List<String> v : vars) {
			if (!pred.test(v)) continue;
			ResultRow rr = new ResultRow();
			for(UnaryFunction<String,List<String>> selector : rowSelectors) {
				rr.addResultColumn(selector.evaluate(v));
			}
			rows.add(rr);
		}
		return (new IntermediateResultSet(cs, rows));
	}

	
	
	// this is in the show/info schema; not the logical schema, hence the pass in
	public static void addScoping(SelectStatement in, TableKey onTK, InformationSchemaColumnView scopeColumn, String filterValue) {
		// add the filter on the appropriate scope
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());
		decompAnd.add(new FunctionCall(FunctionName.makeEquals(),new ColumnInstance(scopeColumn,onTK.toInstance()),LiteralExpression.makeStringLiteral(filterValue)));
		in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
	}

	@SuppressWarnings("unused")
	private ColumnSet buildColumnSet(boolean showScopeName) {		
		ColumnSet cs = new ColumnSet();
		LogicalInformationSchemaColumn[] showColumns =
				(showScopeName ? withScope : withoutScope);
		for(LogicalInformationSchemaColumn column : showColumns) {
			cs.addColumn(column.getName().getUnqualified().get(),
					column.getDataSize(),
					column.getType().getTypeName(),
					column.getDataType());			
		}
		return cs;
	}

	private boolean showScopeName(VariableScope vs) {
		// only for scoped scopes where the scope name is missing
		return (vs.getKind() == VariableScopeKind.SCOPED && vs.getScopeName() == null);
	}

	private VariableScope rebuildScope(String scopeName) {
		if (SESSION_TABLE_NAME.equals(scopeName)) {
			return new VariableScope(VariableScopeKind.SESSION);
		} else if (GLOBAL_TABLE_NAME.equals(scopeName)) {
			return new VariableScope(VariableScopeKind.GLOBAL);
		} else if (SCOPED_TABLE_NAME.equals(scopeName)) {
			return new VariableScope("");
		} else {	
			return new VariableScope(scopeName);
		}
	}
	
	private List<UnaryFunction<String,List<String>>> buildRowSelectors(LogicalQuery lq) {
		// we're going to be applied to <scope>, <variable-name>, <variable-value>.
		// the logical query projection is correct wrt show the scope
		List<UnaryFunction<String,List<String>>> out = new ArrayList<UnaryFunction<String,List<String>>>();
		
		for(ExpressionNode en : lq.getQuery().getProjectionEdge()) {
			ExpressionNode targ = ExpressionUtils.getTarget(en);
			ColumnInstance ci = (ColumnInstance) targ;
			final LogicalInformationSchemaColumn lisc = (LogicalInformationSchemaColumn) ci.getColumn();
			out.add(new UnaryFunction<String,List<String>>() {

				@Override
				public String evaluate(List<String> object) {
					return object.get(lisc.getPosition());
				}
				
			});
			
		}
		return out;
	}
	
}

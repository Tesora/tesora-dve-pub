package com.tesora.dve.sql.infoschema.direct;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.ShowSchemaBehavior;
import com.tesora.dve.sql.infoschema.engine.LogicalSchemaQueryEngine;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.PEQueryVariablesStatement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;

public class DirectVariablesTable extends DirectInformationSchemaTable {

	public static final String GLOBAL_TABLE_NAME = "global_variables";
	public static final String SESSION_TABLE_NAME = "session_variables";
	public static final String SCOPED_TABLE_NAME = "dve_variables";


	private final VariableScope defaultScope;
	
	public DirectVariablesTable(SchemaContext sc, InfoView view,
			List<PEColumn> cols, UnqualifiedName tableName,
			VariableScope defScope,
			List<DirectColumnGenerator> columnGenerators) {
		super(sc, view, cols, tableName, null, false,
				false, columnGenerators);
		this.defaultScope = defScope;
	}

	
	public VariableScope getDefaultScope() {
		return defaultScope;
	}
	
	
	@Override
	public boolean isVariablesTable() {
		return true;
	}

	
	public static Statement execute(SchemaContext sc, DirectVariablesTable tab, VariableScope vs, ViewQuery vq, ProjectionInfo pi) {
		// unlike other direct tables, we never convert this down; instead we just execute it
		// we don't need to scope the query - it's on the table
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(vq.getQuery().getWhereClause());
		String likeExpr = null;
		if (decompAnd.isEmpty()) {
			likeExpr = "";
		} else if (decompAnd.size() == 1) {
			if (decompAnd.get(0) instanceof FunctionCall) {
				FunctionCall fc = (FunctionCall) decompAnd.get(0);
				if (fc.getFunctionName().isLike()) {
					LiteralExpression likeArg = (LiteralExpression) fc.getParametersEdge().get(1);
					likeExpr = (String) likeArg.getValue(sc.getValues());
				}
			}	
		}
		if (likeExpr != null) {
			for(ExpressionNode en : vq.getQuery().getProjectionEdge()) {
				ExpressionNode targ = ExpressionUtils.getTarget(en);
				if (!(targ instanceof ColumnInstance)) {
					likeExpr = null;
					break;
				}
			}
		}
		if (likeExpr != null) {
			IntermediateResultSet irs = executeLikeSelect(sc,likeExpr,vs,vq,pi);
			return new SchemaQueryStatement(false,tab.getName().get(),irs);
		} else {
			return new PEQueryVariablesStatement(vq.getQuery(),vs);
		}		
	}
	
	public static IntermediateResultSet executeLikeSelect(SchemaContext sc,
			String likeExpr, VariableScope vs, ViewQuery vq, ProjectionInfo pi) {
		List<List<String>> vars = null;
		try {
			vars = sc.getConnection().getVariables(vs);
		} catch (PEException e) {
			e.printStackTrace();
			return null;
		}
		List<UnaryFunction<String,List<String>>> rowSelectors = buildRowSelectors(vq);
		List<List<InformationSchemaColumn>> cols =
				Functional.apply(vq.getQuery().getProjection(), new UnaryFunction<List<InformationSchemaColumn>, ExpressionNode>() {

					@Override
					public List<InformationSchemaColumn> evaluate(
							ExpressionNode object) {
						ColumnInstance ci = (ColumnInstance) ExpressionUtils.getTarget(object);
						return Collections.singletonList((InformationSchemaColumn)ci.getColumn());
					}
				}	);
		
		ColumnSet cs = 
				LogicalSchemaQueryEngine.buildProjectionMetadata(sc, cols, pi, Collections.emptyList());
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

	private static UnaryPredicate<List<String>> buildPredicate(String likeExpr) {
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

	private static List<UnaryFunction<String,List<String>>> buildRowSelectors(ViewQuery lq) {
		// we're going to be applied to <scope>, <variable-name>, <variable-value>.
		// the logical query projection is correct wrt show the scope
		List<UnaryFunction<String,List<String>>> out = new ArrayList<UnaryFunction<String,List<String>>>();
		
		for(ExpressionNode en : lq.getQuery().getProjectionEdge()) {
			ExpressionNode targ = ExpressionUtils.getTarget(en);
			ColumnInstance ci = (ColumnInstance) targ;
			final InformationSchemaColumn isc = (InformationSchemaColumn) ci.getColumn();
			final int delta = (isc.getTable() instanceof ShowSchemaBehavior ? 0 : 1);
			out.add(new UnaryFunction<String,List<String>>() {

				@Override
				public String evaluate(List<String> object) {
					return object.get(isc.getPosition()+delta);
				}
				
			});
			
		}
		return out;
	}

}

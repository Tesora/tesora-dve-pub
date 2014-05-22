package com.tesora.dve.sql.infoschema.show;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.VariablesLogicalInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.util.UnaryPredicate;

// show variables is still not really supported.  we return a fake result set.
public class VariablesInformationSchemaTable extends ShowInformationSchemaTable {
	
	protected VariablesLogicalInformationSchemaTable logicalTable = null;
	protected VariableScope vs = null;
	
	public VariablesInformationSchemaTable(VariablesLogicalInformationSchemaTable logicalTable) {
		super(null, logicalTable.getName().getUnqualified(), logicalTable.getName().getUnqualified(), false, false);
		
		this.logicalTable = logicalTable;
		addColumn(null,new InformationSchemaColumnView(InfoView.SHOW, logicalTable.lookup(VariablesLogicalInformationSchemaTable.SCOPE_COL_NAME), new UnqualifiedName(VariablesLogicalInformationSchemaTable.SCOPE_COL_NAME)));
		addColumn(null,new InformationSchemaColumnView(InfoView.SHOW, logicalTable.lookup(VariablesLogicalInformationSchemaTable.NAME_COL_NAME), new UnqualifiedName(VariablesLogicalInformationSchemaTable.NAME_COL_NAME)));
		addColumn(null,new InformationSchemaColumnView(InfoView.SHOW, logicalTable.lookup(VariablesLogicalInformationSchemaTable.VALUE_COL_NAME), new UnqualifiedName(VariablesLogicalInformationSchemaTable.VALUE_COL_NAME)));
	}

	public Statement buildShow(SchemaContext pc, VariableScope vs1,
			ExpressionNode likeExpr, ExpressionNode whereExpr) {
		this.vs = vs1;
		
		return buildShowPlural(pc, null, likeExpr, whereExpr, null);
	}

	@Override
	public IntermediateResultSet executeWhereSelect(SchemaContext sc,
			ExpressionNode wc, List<Name> scoping, ShowOptions options) {
		boolean showScopeName = vs.getScopeKind().hasName() && vs.getScopeName() == null;
		ColumnSet cs = buildColumnSet(showScopeName);

		List<ResultRow> rows = new ArrayList<ResultRow>();
		rows.add(new ResultRow().addResultColumn("character_set_client").addResultColumn("latin1"));
		rows.add(new ResultRow().addResultColumn("character_set_connection").addResultColumn("latin1"));
		rows.add(new ResultRow().addResultColumn("character_set_results").addResultColumn("latin1"));
		rows.add(new ResultRow().addResultColumn("character_set_server").addResultColumn("latin1"));
		rows.add(new ResultRow().addResultColumn("init_connect").addResultColumn(""));
		rows.add(new ResultRow().addResultColumn("interactive_timeout").addResultColumn("28800"));
		rows.add(new ResultRow().addResultColumn("lower_case_table_names").addResultColumn("2"));
		rows.add(new ResultRow().addResultColumn("max_allowed_packet").addResultColumn("16777216"));
		rows.add(new ResultRow().addResultColumn("net_buffer_length").addResultColumn("16384"));
		rows.add(new ResultRow().addResultColumn("net_write_timeout").addResultColumn("60"));
		rows.add(new ResultRow().addResultColumn("query_cache_size").addResultColumn("41943040"));
		rows.add(new ResultRow().addResultColumn("query_cache_type").addResultColumn("ON"));
		rows.add(new ResultRow().addResultColumn("sql_mode").addResultColumn(""));
		rows.add(new ResultRow().addResultColumn("system_time_zone").addResultColumn("EDT"));
		rows.add(new ResultRow().addResultColumn("time_zone").addResultColumn("SYSTEM"));
		rows.add(new ResultRow().addResultColumn("tx_isolation").addResultColumn("REPEATABLE-READ"));
		rows.add(new ResultRow().addResultColumn("wait_timeout").addResultColumn("28800"));

		return new IntermediateResultSet(cs, rows);
	}

	@Override
	public IntermediateResultSet executeLikeSelect(SchemaContext sc,
			String likeExpr, List<Name> scoping, ShowOptions options) {
		List<List<String>> vars = null;
		try {
			vars = sc.getConnection().getVariables(vs);
		} catch (PEException e) {
			return null;
		}
		boolean showScopeName = vs.getScopeKind().hasName() && vs.getScopeName() == null;
		ColumnSet md = buildColumnSet(showScopeName);
		List<ResultRow> rows = new ArrayList<ResultRow>();
		UnaryPredicate<List<String>> pred = buildPredicate(likeExpr);
		for(List<String> v : vars) {
			if (!pred.test(v)) continue;
			ResultRow rr = new ResultRow();
			if (showScopeName)
				rr.addResultColumn(v.get(0));
			rr.addResultColumn(v.get(1));
			rr.addResultColumn(v.get(2));
			rows.add(rr);
		}
		return (new IntermediateResultSet(md, rows));
	}

	@Override
	protected void validate(SchemaView ofView) {
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
	
	private ColumnSet buildColumnSet(boolean showScopeName) {
		LogicalInformationSchemaColumn scope_lc = logicalTable.lookup(VariablesLogicalInformationSchemaTable.SCOPE_COL_NAME);
		LogicalInformationSchemaColumn name_lc = logicalTable.lookup(VariablesLogicalInformationSchemaTable.NAME_COL_NAME);
		LogicalInformationSchemaColumn value_lc = logicalTable.lookup(VariablesLogicalInformationSchemaTable.VALUE_COL_NAME);
		
		ColumnSet cs = new ColumnSet();
		if (showScopeName)
			cs.addColumn(scope_lc.getName().getUnqualified().get(), scope_lc.getDataSize(), scope_lc.getType().getTypeName(), scope_lc.getDataType());
		cs.addColumn(name_lc.getName().getUnqualified().get(), name_lc.getDataSize(), name_lc.getType().getTypeName(), name_lc.getDataType());
		cs.addColumn(value_lc.getName().getUnqualified().get(), value_lc.getDataSize(), value_lc.getType().getTypeName(), value_lc.getDataType());

		return cs;
	}
}

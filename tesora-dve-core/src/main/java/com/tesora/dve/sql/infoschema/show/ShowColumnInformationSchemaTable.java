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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.engine.NamedParameter;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class ShowColumnInformationSchemaTable extends
		ShowInformationSchemaTable {

	public ShowColumnInformationSchemaTable(
			LogicalInformationSchemaTable basedOn, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean isPriviledged, boolean isExtension) {
		super(basedOn, viewName, pluralViewName, isPriviledged, isExtension);
	}

	@Override
	protected void handleScope(SchemaContext sc, SelectStatement ss, Map<String,Object> params, List<Name> scoping) {
		// for columns: table_name, db_name
		if (scoping.size() > 2)
			throw new SchemaException(Pass.SECOND, "Overly qualified show columns statement");
		AbstractInformationSchemaColumnView tableColumn = lookup("table");
		AbstractInformationSchemaColumnView dbColumn = lookup("database");
		TableInstance ti = ss.getBaseTables().get(0);
		Name tablen = scoping.get(0);
		Name dbn = (scoping.size() > 1 ? scoping.get(1) : null);
		if (dbn == null && sc != null) dbn = sc.getCurrentDatabase().getName();
		String tableName = (tablen == null ? null : tablen.get());
		String dbName = (dbn == null ? null : dbn.get());
		ExpressionNode wc = ss.getWhereClause();
		List<ExpressionNode> decomp = ExpressionUtils.decomposeAndClause(wc);
		ColumnInstance tabrefName = tableColumn.buildNameTest(ti);
		FunctionCall tnc = new FunctionCall(FunctionName.makeEquals(),tabrefName,new NamedParameter(new UnqualifiedName("enctab")));
		decomp.add(tnc);
		params.put("enctab",tableName);
		ColumnInstance dbRefName = dbColumn.buildNameTest(ti);
		FunctionCall dnc = new FunctionCall(FunctionName.makeEquals(),dbRefName,new NamedParameter(new UnqualifiedName("encdb")));
		decomp.add(dnc);
		params.put("encdb",dbName);
		ss.setWhereClause(ExpressionUtils.safeBuildAnd(decomp));
	}

	@Override
	public IntermediateResultSet executeWhereSelect(SchemaContext sc, ExpressionNode wc, List<Name> scoping, ShowOptions options) {
		IntermediateResultSet irs = super.executeWhereSelect(sc, wc, scoping, options);
		if (wc == null && scoping != null && scoping.size() == 1 && irs.isEmpty())
			throw new SchemaException(Pass.PLANNER, "No such table: " + scoping.get(0).getSQL());

		if (!options.isFull())
			return irs;
		
		return padResults(irs);
	}

	@Override
	public IntermediateResultSet executeLikeSelect(SchemaContext sc, String likeExpr, List<Name> scoping, ShowOptions options) {
		IntermediateResultSet irs = super.executeLikeSelect(sc, likeExpr, scoping, options);
		if (sc != null && likeExpr == null && scoping != null && scoping.size() == 1 && irs.isEmpty())
			throw new SchemaException(Pass.PLANNER, "No such table: " + scoping.get(0).getSQL());
		
		if (!options.isFull())
			return irs;
		
		return padResults(irs);
	}

	// fake out what we don't support quite yet.
	private IntermediateResultSet padResults(IntermediateResultSet irs) {
		ColumnSet cs = new ColumnSet();
		Map<Integer, String> colInsertMap = new HashMap<Integer, String>();
		for(int c=1; c <= irs.getMetadata().getColumnList().size(); ++c) {
			ColumnMetadata col = irs.getMetadata().getColumn(c);
			cs.addColumn(col);
			if (StringUtils.equalsIgnoreCase(col.getName(), ShowSchema.Column.TYPE)) {
				colInsertMap.put(c, ShowSchema.Column.TYPE);
				cs.addColumn("Collation", 255, "varchar", java.sql.Types.VARCHAR);
			} else if (StringUtils.equalsIgnoreCase(col.getName(), ShowSchema.Column.EXTRA)) {
				colInsertMap.put(c, ShowSchema.Column.EXTRA);
				cs.addColumn("Privileges", 255, "varchar", java.sql.Types.VARCHAR);
				cs.addColumn("Comment", 255, "varchar", java.sql.Types.VARCHAR);
			}
		}
		
		List<ResultRow> rows = new ArrayList<ResultRow>();
		for(ResultRow row : irs.getRows()) {
			ResultRow rr = new ResultRow();
			for(int i=1; i<=row.getRow().size(); ++i) {
				rr.addResultColumn(row.getResultColumn(i));
				if (colInsertMap.containsKey(i)) {
					if (StringUtils.equalsIgnoreCase(colInsertMap.get(i), ShowSchema.Column.TYPE)) {
						rr.addResultColumn(new ResultColumn("", false));
					} else if (StringUtils.equalsIgnoreCase(colInsertMap.get(i), ShowSchema.Column.EXTRA)) {
						rr.addResultColumn(new ResultColumn("", false));
						rr.addResultColumn(new ResultColumn("", false));
					}
				}
			}
			rows.add(rr);
		}
		
		return new IntermediateResultSet(cs, rows);
	}
}

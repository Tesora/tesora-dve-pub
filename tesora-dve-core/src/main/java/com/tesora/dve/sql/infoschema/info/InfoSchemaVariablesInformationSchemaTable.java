package com.tesora.dve.sql.infoschema.info;

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

import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.logical.VariablesLogicalInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class InfoSchemaVariablesInformationSchemaTable extends
		InformationSchemaTableView {

	private InformationSchemaColumnView scope;
	private final String scopeValue;
	
	public InfoSchemaVariablesInformationSchemaTable(
			LogicalInformationSchemaTable basedOn, UnqualifiedName viewName,
			String scopeValue) {
		super(InfoView.INFORMATION, 
				basedOn, viewName, null, false, false);
		this.scopeValue = scopeValue;
	}

	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		VariablesLogicalInformationSchemaTable backing = (VariablesLogicalInformationSchemaTable) getLogicalTable();
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup(VariablesLogicalInformationSchemaTable.NAME_COL_NAME),
				new UnqualifiedName("variable_name")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup(VariablesLogicalInformationSchemaTable.VALUE_COL_NAME),
				new UnqualifiedName("variable_value")));
		scope = new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup(VariablesLogicalInformationSchemaTable.SCOPE_COL_NAME),
				new UnqualifiedName("variable_scope"));
	}
	
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in, TableKey onTK) {
		super.annotate(sc,vq,in,onTK);
		// add the filter on the appropriate scope
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());
		decompAnd.add(new FunctionCall(FunctionName.makeEquals(),new ColumnInstance(scope,onTK.toInstance()),LiteralExpression.makeStringLiteral(scopeValue)));
		in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
	}

	@Override
	public boolean isVariablesTable() {
		return true;
	}

	
}

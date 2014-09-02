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
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.computed.BackedComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaTable;
import com.tesora.dve.sql.infoschema.computed.ConstantComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.logical.catalog.KeyCatalogInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class InfoSchemaTableConstraintsInformationSchemaTable extends
		ComputedInformationSchemaTable {

	protected ComputedInformationSchemaColumn constraintColumn;
	
	public InfoSchemaTableConstraintsInformationSchemaTable(KeyCatalogInformationSchemaTable keyTable) {
		super(InfoView.INFORMATION, keyTable, new UnqualifiedName("table_constraints"), null, false, false);
	}

	@Override
	public void prepare(AbstractInformationSchema schemaView, DBNative dbn) {
		KeyCatalogInformationSchemaTable keyTable = (KeyCatalogInformationSchemaTable) getLogicalTable();
		addColumn(null,new ConstantComputedInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("constraint_catalog"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255), "def"));
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, keyTable.lookup("containing_schema_name"), 
				new UnqualifiedName("constraint_schema")));
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, keyTable.lookup("symbol"),
				new UnqualifiedName("constraint_name")));
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, keyTable.lookup("containing_schema_name"), 
				new UnqualifiedName("table_schema")));
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, keyTable.lookup("containing_table"), 
				new UnqualifiedName("table_name")));
		constraintColumn = new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, keyTable.lookup("constraint_type"),
				new UnqualifiedName("constraint_type")); 
		addColumn(null,constraintColumn);
		
		super.prepare(schemaView, dbn);
	}
	
	@Override
	protected void validate(AbstractInformationSchema ofView) {
	}

	@Override
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in, TableKey onTK) {
		ColumnInstance typeCol = new ColumnInstance(constraintColumn,onTK.toInstance());
		FunctionCall isNotNull = new FunctionCall(FunctionName.makeIsNot(), typeCol, LiteralExpression.makeNullLiteral());
		isNotNull.setGrouped();
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());
		decompAnd.add(isNotNull);
		in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));		
	}

}

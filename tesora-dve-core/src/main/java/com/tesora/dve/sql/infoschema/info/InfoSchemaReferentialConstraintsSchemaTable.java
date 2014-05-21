// OS_STATUS: public
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

import com.tesora.dve.common.catalog.KeyType;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.ConstantSyntheticInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
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

public class InfoSchemaReferentialConstraintsSchemaTable extends
		InformationSchemaTableView {

	// this is the filter column
	InformationSchemaColumnView fksOnly;
	
	public InfoSchemaReferentialConstraintsSchemaTable(KeyCatalogInformationSchemaTable keyTable) {
		super(InfoView.INFORMATION, keyTable, new UnqualifiedName("referential_constraints"), null, false, false);
		fksOnly = new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("constraint"), new UnqualifiedName("constraint_type"));
	}
	
	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		KeyCatalogInformationSchemaTable keyTable = (KeyCatalogInformationSchemaTable) getLogicalTable();
		addColumn(null,new ConstantSyntheticInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("constraint_catalog"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255), "def"));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("containing_schema_name"),
		        new UnqualifiedName("constraint_schema")));
		addColumn(null,new ConstantSyntheticInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("unique_constraint_catalog"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255), "def"));
        addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("referenced_schema_name"),
                new UnqualifiedName("unique_constraint_schema")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("fk_update_action"), 
				new UnqualifiedName("update_rule")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("fk_delete_action"), 
				new UnqualifiedName("delete_rule")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("containing_table"), 
				new UnqualifiedName("table_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, getLogicalTable().lookup("referenced_table_name"), 
				new UnqualifiedName("referenced_table_name")));
		super.prepare(schemaView, dbn);
	}
	
	@Override
	protected void validate(SchemaView ofView) {
	}

	@Override
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in, TableKey onTK) {
		ColumnInstance ref = new ColumnInstance(fksOnly,onTK.toInstance());
		LiteralExpression stringLit = LiteralExpression.makeStringLiteral(KeyType.FOREIGN.getPersistentName());
		FunctionCall filterEx = new FunctionCall(FunctionName.makeEquals(), ref, stringLit);
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());
		decompAnd.add(filterEx);
		in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
	}

	
}

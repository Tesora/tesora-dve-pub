package com.tesora.dve.sql.infoschema.logical.catalog;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.CatalogLogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.SyntheticLogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalCatalogQuery;
import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class ViewCatalogInformationSchemaTable extends
		CatalogInformationSchemaTable {

	private Set<SyntheticLogicalInformationSchemaColumn> synthetics;
	
	public ViewCatalogInformationSchemaTable(Class<?> entKlass,
			InfoSchemaTable anno, String catTabName) {
		super(entKlass, anno, catTabName);
	}

	@Override
	protected void prepare(LogicalInformationSchema schema, DBNative dbn) {
		super.prepare(schema, dbn);
		TableCatalogInformationSchemaTable tableTable = (TableCatalogInformationSchemaTable) schema.lookup("table");
		DatabaseCatalogInformationSchemaTable databaseTable = (DatabaseCatalogInformationSchemaTable) schema.lookup("database");
		CatalogInformationSchemaTable userTable = (CatalogInformationSchemaTable) schema.lookup("user");
		
		final CatalogLogicalInformationSchemaColumn tableColumn = (CatalogLogicalInformationSchemaColumn) lookup("backing");
		final LogicalInformationSchemaColumn tableNameColumn = tableTable.lookup("name");
		final LogicalInformationSchemaColumn tableSchemaColumn = tableTable.lookup("database");
		final LogicalInformationSchemaColumn userColumn = lookup("definedby");
		final LogicalInformationSchemaColumn databaseNameColumn = databaseTable.lookup("name");
		final LogicalInformationSchemaColumn userNameColumn = userTable.lookup("name");
		final LogicalInformationSchemaColumn userAccessColumn = userTable.lookup("access");

		// table_name is the name of the related user_table in the catalog
		SyntheticLogicalInformationSchemaColumn tableName = 
				new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("table_name"), buildStringType(dbn,255)) {

					@Override
					public LanguageNode explode(ColumnInstance subject) {
						return new ScopedColumnInstance(tableNameColumn,buildColumnInstance(subject,tableColumn));
					}
		};
		// table_schema is the name of the schema enclosing the related user_table
		SyntheticLogicalInformationSchemaColumn schemaName =
				new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("table_schema"), buildStringType(dbn,255)) {

					@Override
					public LanguageNode explode(ColumnInstance subject) {
						return new ScopedColumnInstance(databaseNameColumn,new ScopedColumnInstance(tableSchemaColumn,buildColumnInstance(subject,tableColumn)));
					}
			
		};
		// user_name is user@access-spec
		SyntheticLogicalInformationSchemaColumn userName =
				new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("user_name"), buildStringType(dbn,255)) {

					@Override
					public LanguageNode explode(ColumnInstance subject) {
						FunctionName fn = new FunctionName("concat",0,false);
						List<ExpressionNode> params = new ArrayList<ExpressionNode>();
						params.add(new ScopedColumnInstance(userNameColumn,buildColumnInstance(subject,userColumn)));
						params.add(LiteralExpression.makeStringLiteral("@"));
						params.add(new ScopedColumnInstance(userAccessColumn,buildColumnInstance(subject,userColumn)));
						return new FunctionCall(fn,params);
					}
			
		};
		synthetics = new HashSet<SyntheticLogicalInformationSchemaColumn>();
		SyntheticLogicalInformationSchemaColumn[] syns =
				new SyntheticLogicalInformationSchemaColumn[] { tableName, schemaName, userName };
		for(SyntheticLogicalInformationSchemaColumn s : syns) {
			addColumn(null,s);
			synthetics.add(s);
		}
		
		super.prepare(schema,dbn);
	}
	
	@Override
	public boolean isLayered() {
		return true;
	}
	
	@Override
	public boolean requiresRawExecution() {
		return true;
	}

	
	@Override
	public LogicalCatalogQuery explode(SchemaContext sc, LogicalCatalogQuery lq) {
		SelectStatement in = lq.getQuery();
		SyntheticReplacementTraversal sra = new SyntheticReplacementTraversal(synthetics);
		sra.traverse(in);
		return lq;
	}
}

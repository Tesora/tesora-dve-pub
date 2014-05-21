// OS_STATUS: public
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.CatalogInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SyntheticLogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalQuery;
import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.CaseExpression;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.WhenClause;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class KeyCatalogInformationSchemaTable extends
		CatalogInformationSchemaTable {

	// columns we need to build out the synthetic columns
	protected CatalogInformationSchemaColumn containingTable;
	protected CatalogInformationSchemaColumn referencedTable;
	protected CatalogInformationSchemaColumn forwardReferencedTableName;
	protected CatalogInformationSchemaColumn forwardReferencedSchemaName;
	protected CatalogInformationSchemaColumn typeColumn;
	protected CatalogInformationSchemaColumn constraintColumn;
	
	protected SyntheticLogicalInformationSchemaColumn containingTableName;  // containingTable.name
	protected SyntheticLogicalInformationSchemaColumn containingSchemaName; // containingTable.userDatabase.name
	protected SyntheticLogicalInformationSchemaColumn referencedTableName;  // coalesce(referenced_table.name, forwardReferencedTableName)
	protected SyntheticLogicalInformationSchemaColumn referencedSchemaName; // coalesce(referenced_table.userDatabase.name, forwardReferencedSchemaName)
	protected SyntheticLogicalInformationSchemaColumn constraintType;       // case ...?
	
	protected LogicalInformationSchemaColumn databaseForTable;
	protected LogicalInformationSchemaColumn nameForTable;
	protected LogicalInformationSchemaColumn nameForDatabase;
	
	protected Set<SyntheticLogicalInformationSchemaColumn> synthetics;
	
	public KeyCatalogInformationSchemaTable(Class<?> entKlass,
			InfoSchemaTable anno, String catTabName) {
		super(entKlass, anno, catTabName);
	}

	@Override
	protected void prepare(LogicalInformationSchema schema, DBNative dbn) {
		containingTable = (CatalogInformationSchemaColumn) lookup("containing_table");
		referencedTable = (CatalogInformationSchemaColumn) lookup("referenced_table");
		forwardReferencedTableName = (CatalogInformationSchemaColumn) lookup("forward_ref_table_name");
		forwardReferencedSchemaName = (CatalogInformationSchemaColumn) lookup("forward_ref_schema_name");
		typeColumn = (CatalogInformationSchemaColumn) lookup("type");
		constraintColumn = (CatalogInformationSchemaColumn) lookup("constraint");
		
		LogicalInformationSchemaTable tableTable = schema.lookup("table");
		databaseForTable = tableTable.lookup("database");
		LogicalInformationSchemaTable databaseTable = schema.lookup("database");
		nameForDatabase = databaseTable.lookup("name");
		nameForTable = tableTable.lookup("name");


		containingTableName = new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("containing_table_name"), buildStringType(dbn, 255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(nameForTable,buildColumnInstance(subject,containingTable));
			}
			
		};
		
		containingSchemaName = new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("containing_schema_name"), buildStringType(dbn, 255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(nameForDatabase,
						new ScopedColumnInstance(databaseForTable,buildColumnInstance(subject,containingTable)));
			}
			
		};
		
		referencedTableName = new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("referenced_table_name"), buildStringType(dbn, 255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				ColumnInstance existingName = new ScopedColumnInstance(nameForTable, buildColumnInstance(subject,referencedTable));
				ColumnInstance forwardName = buildColumnInstance(subject,forwardReferencedTableName); 
				return new FunctionCall(FunctionName.makeCoalesce(),existingName,forwardName);
			}
			
		};
		
		referencedSchemaName = new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("referenced_schema_name"), buildStringType(dbn, 255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				ColumnInstance existingName = new ScopedColumnInstance(nameForDatabase,
						new ScopedColumnInstance(databaseForTable,buildColumnInstance(subject,referencedTable)));
				ColumnInstance forwardName = buildColumnInstance(subject,forwardReferencedSchemaName); 
				return new FunctionCall(FunctionName.makeCoalesce(),existingName,forwardName);
			}
			
		};

		constraintType = new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("constraint_type"), buildStringType(dbn, 255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				// convert {primary, foreign, unique} to {primary key, foreign key, unique}
				ColumnInstance typeCol = buildColumnInstance(subject,constraintColumn);
				WhenClause pk = new WhenClause(LiteralExpression.makeStringLiteral(ConstraintType.PRIMARY.getSQL()),
						LiteralExpression.makeStringLiteral("PRIMARY KEY"), null);
				WhenClause fk = new WhenClause(LiteralExpression.makeStringLiteral(ConstraintType.FOREIGN.getSQL()),
						LiteralExpression.makeStringLiteral("FOREIGN KEY"), null);
				List<WhenClause> whens = Arrays.asList(new WhenClause[] { pk, fk });
				CaseExpression outerCase = new CaseExpression(typeCol,typeCol,whens,null);
				return outerCase;
			}
			
		};
		
		addColumn(null, containingTableName);
		addColumn(null, containingSchemaName);
		addColumn(null, referencedTableName);
		addColumn(null, referencedSchemaName);
		addColumn(null, constraintType);

		synthetics = new HashSet<SyntheticLogicalInformationSchemaColumn>();
		synthetics.add(containingTableName);
		synthetics.add(containingSchemaName);
		synthetics.add(referencedTableName);
		synthetics.add(referencedSchemaName);
		synthetics.add(constraintType);
		
		super.prepare(schema,dbn);
	}

	@Override
	public boolean requiresRawExecution() {
		return true;
	}
	
	@Override
	public boolean isLayered() {
		return true;
	}
	
	@Override
	public LogicalQuery explode(SchemaContext sc, LogicalQuery lq) {
		SelectStatement in = lq.getQuery();
		new SyntheticReplacementTraversal(synthetics).traverse(in);
		return lq;
	}
}

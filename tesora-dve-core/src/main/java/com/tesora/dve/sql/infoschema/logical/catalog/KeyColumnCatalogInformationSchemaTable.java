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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.CatalogLogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SyntheticLogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalCatalogQuery;
import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.CaseExpression;
import com.tesora.dve.sql.node.expression.CastFunctionCall;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.WhenClause;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class KeyColumnCatalogInformationSchemaTable extends
		CatalogInformationSchemaTable {
	
	// we make a bunch of synthetics available to hide the massaging from the views
	private CatalogLogicalInformationSchemaColumn key;
	private CatalogLogicalInformationSchemaColumn column;
	private CatalogLogicalInformationSchemaColumn referencedColumn;
	private CatalogLogicalInformationSchemaColumn forwardRefColumnName;
	
	private KeyCatalogInformationSchemaTable keyTable; 
	
	private Set<SyntheticLogicalInformationSchemaColumn> synthetics;
	private Set<SyntheticLogicalInformationSchemaColumn> keySynthetics;
	
	public KeyColumnCatalogInformationSchemaTable(Class<?> entKlass,
			InfoSchemaTable anno, String catTabName) {
		super(entKlass, anno, catTabName);
	}

	@Override
	@SuppressWarnings("synthetic-access")
	protected void prepare(LogicalInformationSchema schema, DBNative dbn) {
		keyTable = (KeyCatalogInformationSchemaTable) schema.lookup("key");
		key = (CatalogLogicalInformationSchemaColumn) lookup("containing_key");
		column = (CatalogLogicalInformationSchemaColumn) lookup("column");
		referencedColumn = (CatalogLogicalInformationSchemaColumn) lookup("referenced_column");
		forwardRefColumnName = (CatalogLogicalInformationSchemaColumn) lookup("forward_ref_column_name");
		
		LogicalInformationSchemaTable columnTable = schema.lookup("user_column");
		
		final LogicalInformationSchemaColumn containingTableNameColumn = keyTable.lookup("containing_table_name");
		final LogicalInformationSchemaColumn containingTableSchemaColumn = keyTable.lookup("containing_schema_name");
		final LogicalInformationSchemaColumn referencedTableNameColumn = keyTable.lookup("referenced_table_name");
		final LogicalInformationSchemaColumn referencedSchemaNameColumn = keyTable.lookup("referenced_schema_name");
		final LogicalInformationSchemaColumn columnNameColumn = columnTable.lookup("name");
		
		final LogicalInformationSchemaColumn keyTableNameColumn = keyTable.lookup("name");
		final LogicalInformationSchemaColumn keyTableConstraintColumn = keyTable.lookup("constraint");
		final LogicalInformationSchemaColumn keyTableIndexType = keyTable.lookup("type");
		final LogicalInformationSchemaColumn keyCommentColumn = keyTable.lookup("comment");
		final LogicalInformationSchemaColumn columnTableNullableColumn = columnTable.lookup("nullable");
		final LogicalInformationSchemaColumn keyTableSyntheticColumn = keyTable.lookup("synthetic");

		// containing_key.containing_table_name
		SyntheticLogicalInformationSchemaColumn	containingTableName = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("table_name"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(containingTableNameColumn,buildColumnInstance(subject,key));
			}
			
		};
		// containing_key.containing_schema_name
		SyntheticLogicalInformationSchemaColumn containingTableSchema = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("table_schema"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(containingTableSchemaColumn,buildColumnInstance(subject,key));
			}
			
		};
		// column.name
		SyntheticLogicalInformationSchemaColumn columnName = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("column_name"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(columnNameColumn,buildColumnInstance(subject,column));
			}
			
		};
		// coalesce(referenced_column.name,forward_ref_column_name)
		SyntheticLogicalInformationSchemaColumn	referencedColumnName = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("referenced_column_name"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				ColumnInstance existingName = new ScopedColumnInstance(columnNameColumn,buildColumnInstance(subject,referencedColumn));
				ColumnInstance forwardName = buildColumnInstance(subject,forwardRefColumnName);
				return new FunctionCall(FunctionName.makeCoalesce(),existingName,forwardName);
			}
			
		};
		// containing_key.referenced_table_name
		SyntheticLogicalInformationSchemaColumn referencedTableName = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("referenced_table_name"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(referencedTableNameColumn,buildColumnInstance(subject,key));
			}
			
		};
		// containing_key.referenced_schema_name
		SyntheticLogicalInformationSchemaColumn referencedSchemaName = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("referenced_schema_name"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(referencedSchemaNameColumn,buildColumnInstance(subject,key));
			}
			
		};

		// containing_key.name
		SyntheticLogicalInformationSchemaColumn keyName = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("key_name"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(keyTableNameColumn,buildColumnInstance(subject,key));
			}
			
		};
		// containing_key.constraint
		SyntheticLogicalInformationSchemaColumn constraint = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("constraint"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(keyTableConstraintColumn,buildColumnInstance(subject,key));
			}
			
		};
		// containing_key.index_type
		SyntheticLogicalInformationSchemaColumn indexType = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("index_type"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				return new ScopedColumnInstance(keyTableIndexType,buildColumnInstance(subject,key));
			}
			
		};
		// containing_key.comment
		SyntheticLogicalInformationSchemaColumn keyComment = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("key_comment"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				// apparently we're supposed to return an empty string when there isn't a comment
				return new FunctionCall(FunctionName.makeCoalesce(),
						new ScopedColumnInstance(keyCommentColumn,buildColumnInstance(subject,key)),
						LiteralExpression.makeStringLiteral(""));
			}
		};
		SyntheticLogicalInformationSchemaColumn syntheticKey =
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("synthetic"), buildLongType(dbn)) {

				@Override
				public LanguageNode explode(ColumnInstance subject) {
					return new ScopedColumnInstance(keyTableSyntheticColumn,buildColumnInstance(subject,key));
				}
			
		};
		// column.nullable
		SyntheticLogicalInformationSchemaColumn columnNullable = 
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("nullable"), buildStringType(dbn,255)) {

			@Override
			public LanguageNode explode(ColumnInstance subject) {
				ColumnInstance nullable = new ScopedColumnInstance(columnTableNullableColumn,buildColumnInstance(subject,column));
				ExpressionNode testExpression = nullable;
				WhenClause when = new WhenClause(LiteralExpression.makeLongLiteral(1),LiteralExpression.makeStringLiteral("YES"),null);
				CaseExpression ce = new CaseExpression(testExpression,LiteralExpression.makeStringLiteral(""),Collections.singletonList(when), null);
				return ce;
			}
			
		};
//	    (case kc.key.constraint when 'PRIMARY' then 0 when 'UNIQUE' then 0 else 1) as Non_unique,
		SyntheticLogicalInformationSchemaColumn nonUnique =
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("non_unique"), buildLongType(dbn)) {

				@Override
				public LanguageNode explode(ColumnInstance subject) {
					ScopedColumnInstance constraint1 = new ScopedColumnInstance(keyTableConstraintColumn,buildColumnInstance(subject,key));
					WhenClause pk = 
						new WhenClause(LiteralExpression.makeStringLiteral(ConstraintType.PRIMARY.getSQL()),
									   LiteralExpression.makeLongLiteral(0),null);
					WhenClause uk =
						new WhenClause(LiteralExpression.makeStringLiteral(ConstraintType.UNIQUE.getSQL()),
										LiteralExpression.makeLongLiteral(0),null);
					return new CaseExpression(constraint1,
							LiteralExpression.makeLongLiteral(1),
							Arrays.asList(new WhenClause[] { pk, uk }),
							null);
				}
		};
//	    (case kc.key.index_type when 'FULLTEXT' then null else 'A') as Collation,
		SyntheticLogicalInformationSchemaColumn collation =
			new SyntheticLogicalInformationSchemaColumn(new UnqualifiedName("collation"), buildStringType(dbn,255)) {

				@Override
				public LanguageNode explode(ColumnInstance subject) {
					ScopedColumnInstance indexType1 = new ScopedColumnInstance(keyTableIndexType,buildColumnInstance(subject,key));
					WhenClause ft = 
						new WhenClause(LiteralExpression.makeStringLiteral(IndexType.FULLTEXT.getSQL()),
								new CastFunctionCall(LiteralExpression.makeNullLiteral(), new UnqualifiedName("CHAR")),null);
					return new CaseExpression(indexType1,
							LiteralExpression.makeStringLiteral("A"),
							Collections.singletonList(ft),
							null);
				}
			
		};		
		SyntheticLogicalInformationSchemaColumn[] syns = new SyntheticLogicalInformationSchemaColumn[] {
				containingTableName, containingTableSchema, columnName,
				referencedColumnName, referencedTableName, referencedSchemaName,
				keyName, constraint, indexType, keyComment, columnNullable, nonUnique, collation, 
				syntheticKey
		};
		synthetics = new HashSet<SyntheticLogicalInformationSchemaColumn>();
		keySynthetics = new HashSet<SyntheticLogicalInformationSchemaColumn>();
		for(SyntheticLogicalInformationSchemaColumn s : syns) {
			addColumn(null,s);
			synthetics.add(s);
		}
		keySynthetics = new HashSet<SyntheticLogicalInformationSchemaColumn>(synthetics);
		keySynthetics.remove(columnName);
		keySynthetics.remove(referencedColumnName);
		keySynthetics.remove(columnNullable);
		
		super.prepare(schema, dbn);
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
	public LogicalCatalogQuery explode(SchemaContext sc, LogicalCatalogQuery lq) {
		SelectStatement in = lq.getQuery();
		SyntheticExploder synth = new SyntheticExploder(this);
		synth.traverse(in);
		if (synth.doKeyTable()) {
			keyTable.explode(sc,lq);
		}
		return lq;

	}
	
	@SuppressWarnings("synthetic-access")
	private static class SyntheticExploder extends SyntheticReplacementTraversal {

		KeyColumnCatalogInformationSchemaTable parent;
		boolean doKeyTable;
		
		public SyntheticExploder(KeyColumnCatalogInformationSchemaTable p) {
			super(p.synthetics);
			parent = p;
			doKeyTable = false;
		}
		
		public boolean doKeyTable() {
			return doKeyTable;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			LanguageNode out = super.action(in);
			if (out != in) {
				ColumnInstance ci = (ColumnInstance) in;
				if (parent.keySynthetics.contains(ci.getColumn())) {
					doKeyTable = true;
				}
			}
			return out;
		}
				
	}
}

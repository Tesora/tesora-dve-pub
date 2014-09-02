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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.infoschema.CatalogLogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public class DynamicPolicyCatalogInformationSchemaTable extends
		CatalogInformationSchemaTable {

	public DynamicPolicyCatalogInformationSchemaTable(Class<?> entKlass,
			InfoSchemaTable anno, String catTabName) {
		super(entKlass, anno, catTabName);
	}

	@Override
	public boolean requiresRawExecution() {
		return true;
	}

	@Override
	protected void prepare(LogicalInformationSchema schema, DBNative dbn) {
		for(LogicalInformationSchemaColumn isc : columns)
			isc.prepare(schema, dbn);
		Type stringType = LogicalInformationSchemaTable.buildStringType(dbn, 255);
		Type countType = LogicalInformationSchemaTable.buildLongType(dbn);
		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("agg_provider"), "aggregate_provider",stringType));
		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("agg_class"), "aggregate_class",stringType));
		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("agg_count"), "aggregate_count",countType));

		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("sm_provider"), "small_provider",stringType));
		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("sm_class"), "small_class",stringType));
		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("sm_count"), "small_count",countType));

		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("med_provider"), "medium_provider",stringType));
		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("med_class"), "medium_class",stringType));
		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("med_count"), "medium_count",countType));

		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("l_provider"), "large_provider",stringType));
		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("l_class"), "large_class",stringType));
		addColumn(null,new DynamicPolicyColumnCatalogInformationSchemaColumn(new UnqualifiedName("l_count"), "large_count",countType));
	}

	// work around the embedded attributes by using a custom column representation.  needed because
	// the embedded attributes have a column name but not a field name.
	private static class DynamicPolicyColumnCatalogInformationSchemaColumn extends CatalogLogicalInformationSchemaColumn {
	
		public DynamicPolicyColumnCatalogInformationSchemaColumn(UnqualifiedName logicalName, String columnName, Type type) {
			super(logicalName, type);
			this.columnName = columnName;
			this.isID = false;
			this.nullable = false;
		}
		
		@Override
		public String getFieldName() {
			return null;
		}

		@Override
		public boolean isInjected() {
			return false;
		}

		@Override
		protected Object getRawValue(SchemaContext sc, CatalogEntity ce) {
			throw new SchemaException(Pass.PLANNER, "Unable to obtain value for " + getName().getSQL() + " from object of type " + ce.getClass().getSimpleName() + " because field is not accesible");
		}
		
		@Override
		public void prepare(LogicalInformationSchema schema,DBNative dbn) {
		}
	}
}

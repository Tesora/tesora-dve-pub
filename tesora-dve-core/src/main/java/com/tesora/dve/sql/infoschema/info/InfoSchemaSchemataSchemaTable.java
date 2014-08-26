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

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.ConstantSyntheticInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.ComputedInformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.catalog.DatabaseCatalogInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaSchemataSchemaTable extends
		ComputedInformationSchemaTableView {
	
	protected InformationSchemaColumnView nameColumn;
	
	public InfoSchemaSchemataSchemaTable(DatabaseCatalogInformationSchemaTable schemataTable) {
		super(InfoView.INFORMATION, schemataTable, new UnqualifiedName("schemata"), null, false, false);
	}
	
	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		DatabaseCatalogInformationSchemaTable schemataTable = (DatabaseCatalogInformationSchemaTable) getLogicalTable();
		
		addColumn(null,new ConstantSyntheticInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("catalog_name"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255), "def"));
		
		nameColumn = new InformationSchemaColumnView(InfoView.INFORMATION, schemataTable.lookup("name"), new UnqualifiedName("schema_name")) {
			@Override
			public boolean isIdentColumn() { return true; }
			@Override
			public boolean isOrderByColumn() { return true; }
		}; 

		addColumn(null,nameColumn);
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, schemataTable.lookup("default_storage_group"), 
				new UnqualifiedName("default_persistent_group")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, schemataTable.lookup("template"), 
				new UnqualifiedName("template")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, schemataTable.lookup("template_mode"),
				new UnqualifiedName("template_mode")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, schemataTable.lookup("multitenant"), 
				new UnqualifiedName("multitenant")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, schemataTable.lookup("fkmode"),
				new UnqualifiedName("fkmode")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, schemataTable.lookup("def_character_set_name"), 
				new UnqualifiedName("default_character_set_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, schemataTable.lookup("def_collation_name"), 
				new UnqualifiedName("default_collation_name")));
		super.prepare(schemaView, dbn);
	}
	
	@Override
	protected void validate(SchemaView ofView) {
	}
}

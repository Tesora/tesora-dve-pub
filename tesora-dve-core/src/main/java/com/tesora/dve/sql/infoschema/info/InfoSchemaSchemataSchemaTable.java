// OS_STATUS: public
package com.tesora.dve.sql.infoschema.info;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.ConstantSyntheticInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.catalog.DatabaseCatalogInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaSchemataSchemaTable extends
		InformationSchemaTableView {
	
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

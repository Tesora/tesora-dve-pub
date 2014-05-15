// OS_STATUS: public
package com.tesora.dve.sql.infoschema.info;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.ConstantSyntheticInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.catalog.ViewCatalogInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaViewInformationSchemaTable extends
		InformationSchemaTableView {

	public InfoSchemaViewInformationSchemaTable(LogicalInformationSchemaTable basedOn) {
		super(InfoView.INFORMATION, basedOn, new UnqualifiedName("views"), null, false, false);
	}

	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		ViewCatalogInformationSchemaTable backing = (ViewCatalogInformationSchemaTable) getLogicalTable();
		addColumn(null, new ConstantSyntheticInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("table_catalog"),
				LogicalInformationSchemaTable.buildStringType(dbn,512), "def"));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("table_schema"), new UnqualifiedName("table_schema")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("table_name"), new UnqualifiedName("table_name")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("definition"), new UnqualifiedName("view_definition")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("check"), new UnqualifiedName("check_option")));
		addColumn(null, new ConstantSyntheticInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("is_updatable"),
				LogicalInformationSchemaTable.buildStringType(dbn, 3), "NO"));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("user_name"), new UnqualifiedName("definer")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("security"), new UnqualifiedName("security_type")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("client_charset"), new UnqualifiedName("character_set_client")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("connection_collation"), new UnqualifiedName("collation_connection")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("viewmode"), new UnqualifiedName("mode")));
	}
	
}

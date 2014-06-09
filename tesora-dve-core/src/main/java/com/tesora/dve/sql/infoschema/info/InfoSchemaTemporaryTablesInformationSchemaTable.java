package com.tesora.dve.sql.infoschema.info;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.DistributionLogicalTable;
import com.tesora.dve.sql.infoschema.logical.catalog.CatalogInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public abstract class InfoSchemaTemporaryTablesInformationSchemaTable extends
		InformationSchemaTableView {

	private final boolean includeServer;

	protected InformationSchemaColumnView serverColumn;
	
	public InfoSchemaTemporaryTablesInformationSchemaTable(LogicalInformationSchemaTable basedOn,
			UnqualifiedName viewName, boolean includeServer) {
		super(InfoView.INFORMATION, basedOn, viewName,null,false,false);
		this.includeServer = includeServer;
	}

	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		CatalogInformationSchemaTable backing = (CatalogInformationSchemaTable) getLogicalTable();
		serverColumn = new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("server"),
				new UnqualifiedName("server_name"));
		if (includeServer)
			addColumn(null, serverColumn);
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("session"),
				new UnqualifiedName("session_id")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("dbname"),
				new UnqualifiedName("table_schema")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("name"),
				new UnqualifiedName("table_name")));
		addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, backing.lookup("engine"),
				new UnqualifiedName("engine")));
	}

	
}

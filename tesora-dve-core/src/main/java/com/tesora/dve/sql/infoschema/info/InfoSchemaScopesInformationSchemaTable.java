// OS_STATUS: public
package com.tesora.dve.sql.infoschema.info;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.ScopesInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaScopesInformationSchemaTable extends
		InformationSchemaTableView {

	public InfoSchemaScopesInformationSchemaTable(ScopesInformationSchemaTable sist) {
		super(InfoView.INFORMATION, sist, new UnqualifiedName("scopes"), null, true, true);
	}
	
	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		ScopesInformationSchemaTable sist = (ScopesInformationSchemaTable) getLogicalTable();
		String[] names = new String[] { "table_name", "table_schema", "table_state", "tenant_name", "scope_name" };
		for(String n : names)
			addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, sist.lookup(n),
					new UnqualifiedName(n)));
	}
	
}

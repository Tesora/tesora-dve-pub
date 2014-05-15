// OS_STATUS: public
package com.tesora.dve.sql.infoschema.info;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.DistributionLogicalTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaDistributionsInformationSchemaTable extends InformationSchemaTableView {

	public InfoSchemaDistributionsInformationSchemaTable(DistributionLogicalTable dlt) {
		super(InfoView.INFORMATION, dlt, new UnqualifiedName("distributions"),null,false,true);
	}

	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		DistributionLogicalTable dlt = (DistributionLogicalTable) getLogicalTable();
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("database_name"), 
				new UnqualifiedName("database_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("table_name"),
				new UnqualifiedName("table_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("column_name"),
				new UnqualifiedName("column_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("dvposition"),
				new UnqualifiedName("vector_position")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("model_name"),
				new UnqualifiedName("model_type")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("range_name"),
				new UnqualifiedName("model_name")));		
	}

}

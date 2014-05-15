// OS_STATUS: public
package com.tesora.dve.sql.infoschema.info;

import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaPartitionsInformationSchemaTable extends
		InformationSchemaTableView {

	public InfoSchemaPartitionsInformationSchemaTable(
			LogicalInformationSchemaTable basedOn) {
		super(InfoView.INFORMATION, basedOn, new UnqualifiedName("partitions"),
				null, false, false);
		for (LogicalInformationSchemaColumn lisc : basedOn.getColumns(null)) {
			addColumn(null, new InformationSchemaColumnView(InfoView.SHOW,
					lisc, lisc.getName().getUnqualified()));
		}
	}

	@Override
	protected void validate(SchemaView ofView) {
	}
}

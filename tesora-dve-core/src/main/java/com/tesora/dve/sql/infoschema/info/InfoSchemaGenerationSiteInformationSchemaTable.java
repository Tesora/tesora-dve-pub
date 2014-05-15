// OS_STATUS: public
package com.tesora.dve.sql.infoschema.info;

import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaGenerationSiteInformationSchemaTable extends
		InformationSchemaTableView {

	public InfoSchemaGenerationSiteInformationSchemaTable(LogicalInformationSchemaTable basedOn) {
		super(InfoView.INFORMATION, basedOn, new UnqualifiedName("generation_site"), null, true, true);
		orderByColumn = new InformationSchemaColumnView(InfoView.SHOW, basedOn.lookup("group_name"), new UnqualifiedName("group"));
		addColumn(null,orderByColumn);
		addColumn(null,new InformationSchemaColumnView(InfoView.SHOW, basedOn.lookup("version"), new UnqualifiedName("version")));
		addColumn(null,new InformationSchemaColumnView(InfoView.SHOW, basedOn.lookup("site_name"), new UnqualifiedName("site")));
	}

	@Override
	protected void validate(SchemaView ofView) {
	}

}

package com.tesora.dve.sql.infoschema.info;

import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaGlobalTemporaryTablesInformationSchemaTable extends
		InfoSchemaTemporaryTablesInformationSchemaTable {

	public InfoSchemaGlobalTemporaryTablesInformationSchemaTable(
			LogicalInformationSchemaTable basedOn) {
		super(basedOn, new UnqualifiedName("global_temporary_tables"), true); 
	}

}

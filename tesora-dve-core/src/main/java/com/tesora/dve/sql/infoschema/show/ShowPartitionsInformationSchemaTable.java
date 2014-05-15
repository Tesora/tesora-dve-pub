// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.logical.PartitionsInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowPartitionsInformationSchemaTable extends ShowEmptyInformationSchemaTable {

	public ShowPartitionsInformationSchemaTable(PartitionsInformationSchemaTable backed) {
		super(backed, new UnqualifiedName("partitions"), new UnqualifiedName("partitions"), false, false);
	}

	@Override
	protected void validate(SchemaView ofView) {
	}

}

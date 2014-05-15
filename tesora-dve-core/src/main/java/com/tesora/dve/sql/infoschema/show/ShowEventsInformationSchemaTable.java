// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.logical.EventsInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowEventsInformationSchemaTable extends ShowEmptyInformationSchemaTable {

	public ShowEventsInformationSchemaTable(EventsInformationSchemaTable backed) {
		super(backed, new UnqualifiedName("EVENTS"), new UnqualifiedName("EVENTS"), false, false);
	}

	@Override
	protected void validate(SchemaView ofView) {
		// no name column, so skip validation
	}

}
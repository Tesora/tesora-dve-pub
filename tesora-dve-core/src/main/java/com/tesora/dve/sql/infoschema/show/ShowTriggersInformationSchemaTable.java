// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import com.tesora.dve.sql.infoschema.logical.TriggerInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowTriggersInformationSchemaTable extends ShowEmptyInformationSchemaTable {

	public ShowTriggersInformationSchemaTable(TriggerInformationSchemaTable backed) {
		super(backed, new UnqualifiedName("TRIGGER"), new UnqualifiedName("TRIGGERS"),false,false);
	}

}

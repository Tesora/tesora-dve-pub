// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import com.tesora.dve.sql.infoschema.logical.ProcedureStatusInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowProcedureStatusInformationSchemaTable extends ShowEmptyInformationSchemaTable {

	public ShowProcedureStatusInformationSchemaTable(ProcedureStatusInformationSchemaTable backed) {
		super(backed, new UnqualifiedName(ProcedureStatusInformationSchemaTable.TABLE_NAME),
				new UnqualifiedName(ProcedureStatusInformationSchemaTable.TABLE_NAME), false, false);
	}

}
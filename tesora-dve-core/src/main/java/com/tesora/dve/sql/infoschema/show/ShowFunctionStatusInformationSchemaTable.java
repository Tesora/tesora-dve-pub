// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import com.tesora.dve.sql.infoschema.logical.FunctionStatusInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowFunctionStatusInformationSchemaTable extends ShowEmptyInformationSchemaTable {

	public ShowFunctionStatusInformationSchemaTable(FunctionStatusInformationSchemaTable backed) {
		super(backed, new UnqualifiedName(FunctionStatusInformationSchemaTable.TABLE_NAME),
				new UnqualifiedName(FunctionStatusInformationSchemaTable.TABLE_NAME), false, false);
	}

}
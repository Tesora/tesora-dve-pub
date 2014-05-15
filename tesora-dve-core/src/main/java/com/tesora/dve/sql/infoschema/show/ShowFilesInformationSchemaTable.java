// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.logical.FilesInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowFilesInformationSchemaTable extends ShowEmptyInformationSchemaTable {

	public ShowFilesInformationSchemaTable(FilesInformationSchemaTable backed) {
		super(backed, new UnqualifiedName("files"), new UnqualifiedName("files"), false, false);
	}

	@Override
	protected void validate(SchemaView ofView) {
		// no name column, so skip validation
	}

}

// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowEnginesInformationSchemaTable extends
		ShowInformationSchemaTable {

	public ShowEnginesInformationSchemaTable(
			LogicalInformationSchemaTable basedOn, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean isPriviledged,
			boolean isExtension) {
		super(basedOn, viewName, pluralViewName, isPriviledged, isExtension);
	}
	
}

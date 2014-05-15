// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class PluginsInformationSchemaTable extends
		LogicalInformationSchemaTable {

	public PluginsInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName("plugins"));
		addExplicitColumn("PLUGIN_NAME", buildStringType(dbn,64));
		addExplicitColumn("PLUGIN_VERSION", buildStringType(dbn,20));
		addExplicitColumn("PLUGIN_STATUS", buildStringType(dbn,10));
		addExplicitColumn("PLUGIN_TYPE", buildStringType(dbn,80));
		addExplicitColumn("PLUGIN_TYPE_VERSION", buildStringType(dbn,20));
		addExplicitColumn("PLUGIN_LIBRARY", buildStringType(dbn,64));
		addExplicitColumn("PLUGIN_LIBRARY_VERSION", buildStringType(dbn,20));
		addExplicitColumn("PLUGIN_AUTHOR", buildStringType(dbn,64));
		addExplicitColumn("PLUGIN_DESCRIPTION", buildClobType(dbn, 196605));
		addExplicitColumn("PLUGIN_LICENSE", buildStringType(dbn,80));
		addExplicitColumn("LOAD_OPTION", buildStringType(dbn,64));
	}
}

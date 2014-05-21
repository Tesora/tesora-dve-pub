package com.tesora.dve.sql.infoschema.logical;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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

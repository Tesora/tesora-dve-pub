package com.tesora.dve.sql.infoschema.info;

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
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.computed.BackedComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.catalog.CatalogInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public abstract class InfoSchemaTemporaryTablesInformationSchemaTable extends
		ComputedInformationSchemaTable {

	private final boolean includeServer;

	protected ComputedInformationSchemaColumn sessionColumn;
	
	public InfoSchemaTemporaryTablesInformationSchemaTable(LogicalInformationSchemaTable basedOn,
			UnqualifiedName viewName, boolean includeServer) {
		super(InfoView.INFORMATION, basedOn, viewName,null,false,false);
		this.includeServer = includeServer;
	}

	@Override
	public void prepare(AbstractInformationSchema schemaView, DBNative dbn) {
		CatalogInformationSchemaTable backing = (CatalogInformationSchemaTable) getLogicalTable();
		if (includeServer)
			addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("server"),
					new UnqualifiedName("server_name"))); 
		sessionColumn = new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("session"),
				new UnqualifiedName("session_id"));
		addColumn(null, sessionColumn);
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("dbname"),
				new UnqualifiedName("table_schema")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("name"),
				new UnqualifiedName("table_name")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("engine"),
				new UnqualifiedName("engine")));
	}

	
}

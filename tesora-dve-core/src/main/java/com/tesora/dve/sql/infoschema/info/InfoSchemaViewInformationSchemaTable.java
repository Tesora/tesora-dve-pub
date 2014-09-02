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
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaTable;
import com.tesora.dve.sql.infoschema.computed.ConstantComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.logical.catalog.ViewCatalogInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaViewInformationSchemaTable extends
		ComputedInformationSchemaTable {

	public InfoSchemaViewInformationSchemaTable(LogicalInformationSchemaTable basedOn) {
		super(InfoView.INFORMATION, basedOn, new UnqualifiedName("views"), null, false, false);
	}

	@Override
	public void prepare(AbstractInformationSchema schemaView, DBNative dbn) {
		ViewCatalogInformationSchemaTable backing = (ViewCatalogInformationSchemaTable) getLogicalTable();
		addColumn(null, new ConstantComputedInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("table_catalog"),
				LogicalInformationSchemaTable.buildStringType(dbn,512), "def"));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("table_schema"), new UnqualifiedName("table_schema")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("table_name"), new UnqualifiedName("table_name")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("definition"), new UnqualifiedName("view_definition")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("check"), new UnqualifiedName("check_option")));
		addColumn(null, new ConstantComputedInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("is_updatable"),
				LogicalInformationSchemaTable.buildStringType(dbn, 3), "NO"));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("user_name"), new UnqualifiedName("definer")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("security"), new UnqualifiedName("security_type")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("client_charset"), new UnqualifiedName("character_set_client")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("connection_collation"), new UnqualifiedName("collation_connection")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.INFORMATION, backing.lookup("viewmode"), new UnqualifiedName("mode")));
	}
	
}

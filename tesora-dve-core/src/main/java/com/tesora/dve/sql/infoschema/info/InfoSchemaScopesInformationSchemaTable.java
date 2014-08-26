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
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.ComputedInformationSchemaTableView;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.ScopesInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaScopesInformationSchemaTable extends
		ComputedInformationSchemaTableView {

	public InfoSchemaScopesInformationSchemaTable(ScopesInformationSchemaTable sist) {
		super(InfoView.INFORMATION, sist, new UnqualifiedName("scopes"), null, true, true);
	}
	
	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		ScopesInformationSchemaTable sist = (ScopesInformationSchemaTable) getLogicalTable();
		String[] names = new String[] { "table_name", "table_schema", "table_state", "tenant_name", "scope_name" };
		for(String n : names)
			addColumn(null, new InformationSchemaColumnView(InfoView.INFORMATION, sist.lookup(n),
					new UnqualifiedName(n)));
	}
	
}

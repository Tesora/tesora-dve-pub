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
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.ConstantSyntheticInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.ComputedInformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaColumnsInformationSchemaTable extends
		ComputedInformationSchemaTableView {

	// have to use the regular contructor so the reflection lookup works
	public InfoSchemaColumnsInformationSchemaTable(InfoView view,
			LogicalInformationSchemaTable basedOn, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean requiresPriviledge,
			boolean isExtension) {
		super(view, basedOn, viewName, pluralViewName, requiresPriviledge, isExtension);
	}

	@Override
	public void prepare(SchemaView view, DBNative dbn) {
		// add the table_catalog synthetic column
		addColumn(null,new ConstantSyntheticInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("table_catalog"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255), "def"));
		// we don't support column level privileges at all yet, or for that matter differentiated privileges
		// so just return a "" for now.
		addColumn(null,new ConstantSyntheticInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("privileges"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255), ""));

		super.prepare(view, dbn);
	}
	
	@Override
	public void freeze() {
		// really shouldn't do this, but we also should get rid of entity queries.
		AbstractInformationSchemaColumnView tableCatalog = lookup("table_catalog");
		AbstractInformationSchemaColumnView privileges = lookup("privileges");
		AbstractInformationSchemaColumnView tableSchema = lookup("table_schema");
		
		columns.remove(tableCatalog);
		columns.remove(privileges);
		columns.remove(tableSchema);
		columns.add(0, tableSchema);
		columns.add(0, tableCatalog);		
		columns.add(16, privileges);
		
		super.freeze();
	}
	
}

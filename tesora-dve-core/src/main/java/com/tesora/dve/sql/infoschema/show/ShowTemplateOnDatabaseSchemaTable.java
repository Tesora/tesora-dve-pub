package com.tesora.dve.sql.infoschema.show;

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

import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowTemplateOnDatabaseSchemaTable extends
		ShowInformationSchemaTable {

	public ShowTemplateOnDatabaseSchemaTable(ShowInformationSchemaTable databaseView) {
		super(databaseView.getLogicalTable(), 
				new UnqualifiedName("template on database"), new UnqualifiedName("template on databases"), true, true);
		for (final AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn> iscv : databaseView.getColumns(null)) {
			final String columnName = iscv.getLogicalColumn().getColumnName();
			if (columnName.equalsIgnoreCase("name")
					|| columnName.equalsIgnoreCase("template")
					|| columnName.equalsIgnoreCase("template_mode")) {
				addColumn(null, (InformationSchemaColumnView) iscv.copy());
			}
		}
	}

	@Override
	protected boolean useExtensions(SchemaContext sc) {
		return true;
	}
}

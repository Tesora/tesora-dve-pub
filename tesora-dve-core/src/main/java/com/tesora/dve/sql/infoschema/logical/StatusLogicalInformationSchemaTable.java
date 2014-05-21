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
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.BasicType;

public class StatusLogicalInformationSchemaTable extends LogicalInformationSchemaTable {
	public static final String TABLE_NAME = "status";
	public static final String NAME_COL_NAME = "Variable_name";
	public static final String VALUE_COL_NAME = "Value";
	
	protected LogicalInformationSchemaColumn nameColumn = null;
	protected LogicalInformationSchemaColumn valueColumn = null;
	
	public StatusLogicalInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName(TABLE_NAME));
		nameColumn = new LogicalInformationSchemaColumn(new UnqualifiedName(NAME_COL_NAME), BasicType.buildType(java.sql.Types.VARCHAR, 255, dbn));
		valueColumn = new LogicalInformationSchemaColumn(new UnqualifiedName(VALUE_COL_NAME), BasicType.buildType(java.sql.Types.VARCHAR, 255, dbn));
		nameColumn.setTable(this);
		valueColumn.setTable(this);
		addColumn(null,nameColumn);
		addColumn(null,valueColumn);
	}
	
	@Override
	public String getTableName() {
		return TABLE_NAME;
	}
}

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

public class TriggerInformationSchemaTable extends
		LogicalInformationSchemaTable {

	private LogicalInformationSchemaColumn nameColumn;
	
	public TriggerInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName("TRIGGER"));
		nameColumn = new LogicalInformationSchemaColumn(new UnqualifiedName("Name"), buildStringType(dbn, 128)) {
			@Override
			public boolean isID() { return true; }
		}; 
		addColumn(null, nameColumn);
		addExplicitColumn("Trigger", buildStringType(dbn, 64));
		addExplicitColumn("Event", buildStringType(dbn, 6));
		addExplicitColumn("Table", buildStringType(dbn,64));
		addExplicitColumn("Statement", buildClobType(dbn, 196605));
		addExplicitColumn("Timing", buildStringType(dbn,6));
		addExplicitColumn("Created", BasicType.buildType(java.sql.Types.TIMESTAMP, 0, dbn));
		addExplicitColumn("sql_mode", buildStringType(dbn,8192));
		addExplicitColumn("Definier", buildStringType(dbn,80));
	}
	
	@Override
	public LogicalInformationSchemaColumn getNameColumn() {
		return nameColumn;
	}
	
}

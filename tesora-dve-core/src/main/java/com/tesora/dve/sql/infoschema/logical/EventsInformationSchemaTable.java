// OS_STATUS: public
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
import com.tesora.dve.sql.schema.types.BasicType;

public class EventsInformationSchemaTable extends
		LogicalInformationSchemaTable {
	
	public EventsInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName("events"));
		addExplicitColumn("EVENT_CATALOG", buildStringType(dbn, 64));
		addExplicitColumn("EVENT_SCHEMA", buildStringType(dbn, 64));
		addExplicitColumn("EVENT_NAME", buildStringType(dbn, 64));
		addExplicitColumn("DEFINER", buildStringType(dbn, 77));
		addExplicitColumn("TIME_ZONE", buildStringType(dbn, 64));
		addExplicitColumn("EVENT_BODY", buildStringType(dbn, 8));
		addExplicitColumn("EVENT_DEFINITION", buildClobType(dbn, 196605));
		addExplicitColumn("EVENT_TYPE", buildStringType(dbn, 9));
		addExplicitColumn("EXECUTE_AT", buildDateTimeType(dbn));
		addExplicitColumn("INTERVAL_VALUE", buildStringType(dbn, 256));
		addExplicitColumn("INTERVAL_FIELD", buildStringType(dbn, 18));
		addExplicitColumn("SQL_MODE", buildStringType(dbn, 8192));
		addExplicitColumn("STARTS", buildDateTimeType(dbn));
		addExplicitColumn("ENDS", buildDateTimeType(dbn));
		addExplicitColumn("STATUS", buildStringType(dbn, 18));
		addExplicitColumn("ON_COMPLETION", buildStringType(dbn, 12));
		addExplicitColumn("CREATED", buildDateTimeType(dbn));
		addExplicitColumn("LAST_ALTERED", buildDateTimeType(dbn));
		addExplicitColumn("LAST_EXECUTED", buildDateTimeType(dbn));
		addExplicitColumn("EVENT_COMMENT", buildStringType(dbn, 64));
		addExplicitColumn("ORIGINATOR", BasicType.buildType(java.sql.Types.BIGINT, 10, dbn));
		addExplicitColumn("CHARACTER_SET_CLIENT", buildStringType(dbn, 32));
		addExplicitColumn("COLLATION_CONNECTION", buildStringType(dbn, 32));
		addExplicitColumn("DATABASE_COLLATION", buildStringType(dbn, 32));
	}
}
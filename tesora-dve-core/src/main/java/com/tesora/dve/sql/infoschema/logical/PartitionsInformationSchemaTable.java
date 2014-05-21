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

public class PartitionsInformationSchemaTable extends
		LogicalInformationSchemaTable {

	public PartitionsInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName("partitions"));
		addExplicitColumn("TABLE_CATALOG", buildStringType(dbn,512));
		addExplicitColumn("TABLE_SCHEMA", buildStringType(dbn,64));
		addExplicitColumn("TABLE_NAME", buildStringType(dbn,64));
		addExplicitColumn("PARTITION_NAME", buildStringType(dbn,64));
		addExplicitColumn("SUBPARTITION_NAME", buildStringType(dbn,64));
		addExplicitColumn("PARTITION_ORDINAL_POSITION", buildLongType(dbn));
		addExplicitColumn("SUBPARTITION_ORDINAL_POSITION", buildLongType(dbn));
		addExplicitColumn("PARTITION_METHOD", buildStringType(dbn,18));
		addExplicitColumn("SUBPARTITION_METHOD", buildStringType(dbn,12));
		addExplicitColumn("PARTITION_EXPRESSION", buildClobType(dbn, 196605));
		addExplicitColumn("SUBPARTITION_EXPRESSION", buildClobType(dbn, 196605));
		addExplicitColumn("PARTITION_DESCRIPTION", buildClobType(dbn, 196605));
		addExplicitColumn("TABLE_ROWS", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("AVG_ROW_LENGTH", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("DATA_LENGTH", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("MAX_DATA_LENGTH", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("INDEX_LENGTH", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("DATA_FREE", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("CREATE_TIME", buildDateTimeType(dbn));
		addExplicitColumn("UPDATE_TIME", buildDateTimeType(dbn));
		addExplicitColumn("CHECK_TIME", buildDateTimeType(dbn));
		addExplicitColumn("CHECKSUM", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("PARTITION_COMMENT", buildStringType(dbn,80));
		addExplicitColumn("NODEGROUP", buildStringType(dbn,12));
		addExplicitColumn("TABLESPACE_NAME", buildStringType(dbn,64));
	}
}

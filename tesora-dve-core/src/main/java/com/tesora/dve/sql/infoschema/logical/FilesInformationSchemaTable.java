// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.BasicType;

public class FilesInformationSchemaTable extends LogicalInformationSchemaTable{

	public FilesInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName("files"));
		addExplicitColumn("FILE_ID", buildLongType(dbn));
		addExplicitColumn("FILE_NAME", buildStringType(dbn,64));
		addExplicitColumn("FILE_TYPE", buildStringType(dbn,20));
		addExplicitColumn("TABLESPACE_NAME", buildStringType(dbn,64));
		addExplicitColumn("TABLE_CATALOG", buildStringType(dbn,64));
		addExplicitColumn("TABLE_SCHEMA", buildStringType(dbn,64));
		addExplicitColumn("TABLE_NAME", buildStringType(dbn,64));
		addExplicitColumn("LOGFILE_GROUP_NAME", buildStringType(dbn,64));
		addExplicitColumn("LOGFILE_GROUP_NUMBER", buildLongType(dbn));
		addExplicitColumn("ENGINE", buildStringType(dbn,64));
		addExplicitColumn("FULLTEXT_KEYS", buildStringType(dbn,64));
		addExplicitColumn("DELETED_ROWS", buildLongType(dbn));
		addExplicitColumn("UPDATE_COUNT", buildLongType(dbn));
		addExplicitColumn("FREE_EXTENTS", buildLongType(dbn));
		addExplicitColumn("TOTAL_EXTENTS", buildLongType(dbn));
		addExplicitColumn("EXTENT_SIZE", buildLongType(dbn));
		addExplicitColumn("INITIAL_SIZE", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("MAXIMUM_SIZE", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("AUTOEXTEND_SIZE", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("CREATION_TIME", buildDateTimeType(dbn));
		addExplicitColumn("LAST_UPDATE_TIME", buildDateTimeType(dbn));
		addExplicitColumn("LAST_ACCESS_TIME", buildDateTimeType(dbn));
		addExplicitColumn("RECOVER_TIME", buildLongType(dbn));
		addExplicitColumn("TRANSACTION_COUNTER", buildLongType(dbn));
		addExplicitColumn("VERSION", BasicType.buildType(java.sql.Types.BIGINT, 21, dbn));
		addExplicitColumn("ROW_FORMAT", buildStringType(dbn,10));
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
		addExplicitColumn("STATUS", buildStringType(dbn,20));
		addExplicitColumn("EXTRA", buildStringType(dbn,255));
	}
}

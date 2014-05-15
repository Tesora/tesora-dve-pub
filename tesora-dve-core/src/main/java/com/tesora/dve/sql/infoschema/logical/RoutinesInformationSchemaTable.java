// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.BasicType;

public class RoutinesInformationSchemaTable extends
		LogicalInformationSchemaTable {
	
	public RoutinesInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName("routines"));
		addExplicitColumn("SPECIFIC_NAME", buildStringType(dbn, 64));
		addExplicitColumn("ROUTINE_CATALOG", buildStringType(dbn, 512));
		addExplicitColumn("ROUTINE_SCHEMA", buildStringType(dbn, 64));
		addExplicitColumn("ROUTINE_NAME", buildStringType(dbn, 64));
		addExplicitColumn("ROUTINE_TYPE", buildStringType(dbn, 9));
		addExplicitColumn("DATA_TYPE", buildStringType(dbn, 64));
		addExplicitColumn("CHARACTER_MAXIMUM_LENGTH", BasicType.buildType(java.sql.Types.INTEGER, 21, dbn));
		addExplicitColumn("CHARACTER_OCTET_LENGTH", BasicType.buildType(java.sql.Types.INTEGER, 21, dbn));
		addExplicitColumn("NUMERIC_PRECISION", BasicType.buildType(java.sql.Types.INTEGER, 21, dbn));
		addExplicitColumn("NUMERIC_SCALE", BasicType.buildType(java.sql.Types.INTEGER, 21, dbn));
		addExplicitColumn("CHARACTER_SET_NAME", buildStringType(dbn, 64));
		addExplicitColumn("COLLATION_NAME", buildStringType(dbn, 64));
		addExplicitColumn("DTD_IDENTIFIER", buildClobType(dbn, 196605));
		addExplicitColumn("ROUTINE_BODY", buildStringType(dbn, 8));
		addExplicitColumn("ROUTINE_DEFINITION", buildClobType(dbn, 196605));
		addExplicitColumn("EXTERNAL_NAME", buildStringType(dbn, 64));
		addExplicitColumn("EXTERNAL_LANGUAGE", buildStringType(dbn, 64));
		addExplicitColumn("PARAMETER_STYLE", buildStringType(dbn, 8));
		addExplicitColumn("IS_DETERMINISTIC", buildStringType(dbn, 3));
		addExplicitColumn("SQL_DATA_ACCESS", buildStringType(dbn, 64));
		addExplicitColumn("SQL_PATH", buildStringType(dbn, 64));
		addExplicitColumn("SECURITY_TYPE", buildStringType(dbn, 7));
		addExplicitColumn("CREATED", buildDateTimeType(dbn));
		addExplicitColumn("LAST_ALTERED", buildDateTimeType(dbn));
		addExplicitColumn("SQL_MODE", buildStringType(dbn, 8192));
		addExplicitColumn("ROUTINE_COMMENT", buildClobType(dbn, 196605));
		addExplicitColumn("DEFINER", buildStringType(dbn, 77));
		addExplicitColumn("CHARACTER_SET_CLIENT", buildStringType(dbn, 32));
		addExplicitColumn("COLLATION_CONNECTION", buildStringType(dbn, 32));
		addExplicitColumn("DATABASE_COLLATION", buildStringType(dbn, 32));
	}
}
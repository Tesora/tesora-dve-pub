// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ProcedureStatusInformationSchemaTable extends
		LogicalInformationSchemaTable {

	public static final String TABLE_NAME = "PROCEDURE STATUS";

	private LogicalInformationSchemaColumn dbColumn;

	public ProcedureStatusInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName(TABLE_NAME));
		dbColumn = new LogicalInformationSchemaColumn(
				new UnqualifiedName("Db"), buildStringType(dbn, 64)) {
			@Override
			public boolean isID() {
				return true;
			}
		};
		addColumn(null, dbColumn);
		addExplicitColumn("Name", buildStringType(dbn, 64));
		addExplicitColumn("Type", buildStringType(dbn, 9));
		addExplicitColumn("Definer", buildStringType(dbn, 77));
		addExplicitColumn("Modified", buildDateTimeType(dbn));
		addExplicitColumn("Created", buildDateTimeType(dbn));
		addExplicitColumn("Security_type", buildStringType(dbn, 7));
		addExplicitColumn("Comment", buildClobType(dbn, 196605));
		addExplicitColumn("character_set_client", buildStringType(dbn, 32));
		addExplicitColumn("collation_connection", buildStringType(dbn, 32));
		addExplicitColumn("Database Collation", buildStringType(dbn, 32));
	}

	@Override
	public LogicalInformationSchemaColumn getIdentOrderByColumn() {
		return dbColumn;
	}

}
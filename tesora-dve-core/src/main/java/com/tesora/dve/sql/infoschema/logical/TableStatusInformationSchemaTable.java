// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class TableStatusInformationSchemaTable extends LogicalInformationSchemaTable {

	private LogicalInformationSchemaColumn nameColumn;
	
	public TableStatusInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName("TABLE STATUS"));
		nameColumn = new LogicalInformationSchemaColumn(new UnqualifiedName("Name"), buildStringType(dbn, 128)) {
			@Override
			public boolean isID() { return true; }
		}; 
		addColumn(null, nameColumn);
		addExplicitColumn("Engine", buildStringType(dbn, 16));
		addExplicitColumn("Version", buildLongType(dbn));
		addExplicitColumn("Row_format", buildStringType(dbn,16));
		addExplicitColumn("Rows", buildLongType(dbn));	
		addExplicitColumn("Avg_row_length", buildLongType(dbn));
		addExplicitColumn("Data_length", buildLongType(dbn));
		addExplicitColumn("Max_data_length", buildLongType(dbn));
		addExplicitColumn("Index_length", buildLongType(dbn));
		addExplicitColumn("Data_free", buildLongType(dbn));
		addExplicitColumn("Auto_increment", buildLongType(dbn));
		addExplicitColumn("Create_time", buildDateTimeType(dbn));
		addExplicitColumn("Update_time", buildDateTimeType(dbn));
		addExplicitColumn("Check_time", buildDateTimeType(dbn));
		addExplicitColumn("Collation", buildStringType(dbn,32));
		addExplicitColumn("Checksum", buildLongType(dbn));
		addExplicitColumn("Create_options", buildStringType(dbn,128));
		addExplicitColumn("Comment", buildStringType(dbn,128));
	}

	
	@Override
	public LogicalInformationSchemaColumn getNameColumn() {
		return nameColumn;
	}
}

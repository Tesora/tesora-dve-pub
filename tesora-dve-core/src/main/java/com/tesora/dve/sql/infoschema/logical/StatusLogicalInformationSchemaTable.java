// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

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

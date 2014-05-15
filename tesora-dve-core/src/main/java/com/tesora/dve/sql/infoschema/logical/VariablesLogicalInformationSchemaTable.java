// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.types.BasicType;

public class VariablesLogicalInformationSchemaTable extends LogicalInformationSchemaTable {
	public static final String TABLE_NAME = "variables";
	public static final String SCOPE_COL_NAME = "Scope";
	public static final String NAME_COL_NAME = "Variable_name";
	public static final String VALUE_COL_NAME = "Value";
	
	protected LogicalInformationSchemaColumn scopeColumn = null;
	protected LogicalInformationSchemaColumn nameColumn = null;
	protected LogicalInformationSchemaColumn valueColumn = null;
	
	protected VariableScope vs = null;
	
	public VariablesLogicalInformationSchemaTable(DBNative dbn) {
		super(new UnqualifiedName(TABLE_NAME));
		scopeColumn = new LogicalInformationSchemaColumn(new UnqualifiedName(SCOPE_COL_NAME), BasicType.buildType(java.sql.Types.VARCHAR, 255, dbn));
		nameColumn = new LogicalInformationSchemaColumn(new UnqualifiedName(NAME_COL_NAME), BasicType.buildType(java.sql.Types.VARCHAR, 255, dbn));
		valueColumn = new LogicalInformationSchemaColumn(new UnqualifiedName(VALUE_COL_NAME), BasicType.buildType(java.sql.Types.VARCHAR, 255, dbn));
		scopeColumn.setTable(this);
		nameColumn.setTable(this);
		valueColumn.setTable(this);
		addColumn(null,scopeColumn);
		addColumn(null,nameColumn);
		addColumn(null,valueColumn);
	}
	
	@Override
	public String getTableName() {
		return TABLE_NAME;
	}
}

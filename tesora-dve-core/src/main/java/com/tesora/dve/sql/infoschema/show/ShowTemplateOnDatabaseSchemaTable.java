// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowTemplateOnDatabaseSchemaTable extends
		ShowInformationSchemaTable {

	public ShowTemplateOnDatabaseSchemaTable(ShowInformationSchemaTable databaseView) {
		super(databaseView.getLogicalTable(), 
				new UnqualifiedName("template on database"), new UnqualifiedName("template on databases"), true, true);
		for (final AbstractInformationSchemaColumnView iscv : databaseView.getColumns(null)) {
			final String columnName = iscv.getLogicalColumn().getColumnName();
			if (columnName.equalsIgnoreCase("name")
					|| columnName.equalsIgnoreCase("template")
					|| columnName.equalsIgnoreCase("template_mode")) {
				addColumn(null, iscv.copy());
			}
		}
	}

	@Override
	protected boolean useExtensions(SchemaContext sc) {
		return true;
	}
}

// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl.alter;

import java.util.List;

import com.tesora.dve.sql.schema.PEColumn;

public abstract class AbstractAlterColumnAction extends AlterTableAction {

	public AbstractAlterColumnAction() {
	}

	@Override
	public AlterTargetKind getTargetKind() {
		return AlterTargetKind.COLUMN;
	}
	
	// for adds, this can be the new column; for everything else the old column
	public abstract List<PEColumn> getColumns();
}

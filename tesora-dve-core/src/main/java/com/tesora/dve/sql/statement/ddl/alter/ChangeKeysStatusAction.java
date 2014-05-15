// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl.alter;

import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class ChangeKeysStatusAction extends AlterTableAction {

	private final boolean enable;
	
	public ChangeKeysStatusAction(boolean en) {
		super();
		enable = en;
	}
	
	public boolean isEnable() {
		return enable;
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		// does nothing
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		return false;
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		return null;
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		return new ChangeKeysStatusAction(enable);
	}

	@Override
	public AlterTargetKind getTargetKind() {
		return AlterTargetKind.TABLE;
	}

	@Override
	public Action getActionKind() {
		return Action.ALTER;
	}

	@Override
	public boolean isPassthrough() {
		return true;
	}

	
}

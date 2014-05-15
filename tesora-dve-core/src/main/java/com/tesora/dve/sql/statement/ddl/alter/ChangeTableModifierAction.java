// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl.alter;

import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.modifiers.TableModifier;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class ChangeTableModifierAction extends AlterTableAction {

	private TableModifier modifier;
	
	public ChangeTableModifierAction(TableModifier tm) {
		super();
		modifier = tm;
	}
	
	public TableModifier getModifier() {
		return modifier;
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		tab.alterModifier(modifier);
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
		return new ChangeTableModifierAction(modifier);
	}

	@Override
	public AlterTargetKind getTargetKind() {
		return AlterTargetKind.TABLE;
	}

	@Override
	public Action getActionKind() {
		return Action.ALTER;
	}

}

// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl.alter;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class AlterColumnAction extends AbstractAlterColumnAction {

	protected PEColumn alteredColumn;
	protected LiteralExpression defEx;

	public AlterColumnAction(PEColumn newDef, LiteralExpression defaultExpression) {
		alteredColumn = newDef;
		defEx = defaultExpression;
	}
	
	public PEColumn getAlteredColumn() {
		return alteredColumn;
	}
	
	public LiteralExpression getNewDefault() {
		return defEx;
	}
	
	public boolean isDropDefault() {
		return defEx == null;
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		PEColumn c = alteredColumn.getIn(sc, tab);
		c.setDefaultValue(defEx);
		c.normalize();
		return null;
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		// altering a column into it's current definition is ok
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		// let's assume never for now
		return false;
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		return new AlterColumnAction(alteredColumn, defEx);
	}

	@Override
	public Action getActionKind() {
		return Action.ALTER;
	}

	@Override
	public List<PEColumn> getColumns() {
		return Collections.singletonList(alteredColumn);
	}

}

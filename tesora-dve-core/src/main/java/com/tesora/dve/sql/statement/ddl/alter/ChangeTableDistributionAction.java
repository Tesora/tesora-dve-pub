// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl.alter;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.ddl.AlterTableDistributionStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterStatement;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class ChangeTableDistributionAction extends AlterTableAction {

	private DistributionVector newVector;
	
	public ChangeTableDistributionAction(DistributionVector nv) {
		super();
		newVector = nv;
	}
	
	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		// it's a noop if the table is already distributed like the vector indicates
		return false;
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		return new ChangeTableDistributionAction(newVector); 
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
	public boolean hasSQL(SchemaContext sc, PETable pet) {
		return false;
	}

	@Override
	public PEAlterStatement<PETable> requiresSingleStatement(SchemaContext sc, TableKey target) {
		if (target instanceof MTTableKey) 
			throw new SchemaException(Pass.SECOND, "No support for alter dist model of tenant table");
		if (target.getAbstractTable().isView())
			throw new SchemaException(Pass.SECOND, "No support for alter dist on a view");
		return new AlterTableDistributionStatement(sc, target.getAbstractTable().asTable(),newVector);
	}

}

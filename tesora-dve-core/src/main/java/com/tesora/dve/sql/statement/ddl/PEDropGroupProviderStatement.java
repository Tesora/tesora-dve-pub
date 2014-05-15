// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEProvider;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.PassThroughCommand.Command;
import com.tesora.dve.sql.util.Pair;

public class PEDropGroupProviderStatement extends PEGroupProviderDDLStatement {

	public PEDropGroupProviderStatement(PEProvider pep,
			List<Pair<Name, LiteralExpression>> opts) {
		super(pep, opts);
	}

	@Override
	public Action getAction() {
		return Action.DROP;
	}

	// set up my delete objects
	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext pc) throws PEException {
		return Collections.singletonList((CatalogEntity)getBackingProvider(pc));
	}

	@Override
	public Command getCommand() {
		return Command.DROP;
	}

}

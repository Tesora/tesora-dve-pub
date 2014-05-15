// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.List;

import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEProvider;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.PassThroughCommand.Command;
import com.tesora.dve.sql.util.Pair;

public class PECreateGroupProviderStatement extends PEGroupProviderDDLStatement {

	public PECreateGroupProviderStatement(
			PEProvider provider, List<Pair<Name,LiteralExpression>> opts) {
		super(provider, opts);
	}

	@Override
	public Action getAction() {
		return Action.CREATE;
	}
	
	@Override
	public Command getCommand() {
		return Command.CREATE;
	}

}

// OS_STATUS: public
package com.tesora.dve.queryplan;

import java.sql.Types;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepGetScopedVariableOperation extends QueryStepOperation {
	
	String scopeName;
	String variableName;
	String alias;
	
	public QueryStepGetScopedVariableOperation(String scopeName, String variableName, String alias) {
		this.scopeName = scopeName;
		this.variableName = variableName;
		this.alias = alias;
	}

	public QueryStepGetScopedVariableOperation(String scopeName, String variableName) {
		this(scopeName, variableName, variableName);
	}

	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
        resultConsumer.inject(ColumnSet.singleColumn(alias, Types.VARCHAR),
				ResultRow.singleRow(Singletons.require(HostService.class).getScopedVariable(scopeName, variableName)));
	}

}

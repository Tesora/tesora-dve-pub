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

public class QueryStepGetGlobalVariableOperation extends QueryStepOperation {
	
	String variableName;
	private String alias;


	public QueryStepGetGlobalVariableOperation(String variableName, String alias) {
		super();
		this.variableName = variableName;
		this.alias = alias;
	}
	
	public QueryStepGetGlobalVariableOperation(String variableName) {
		this(variableName, variableName);
	}


	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
        resultConsumer.inject(ColumnSet.singleColumn(alias, Types.VARCHAR),
				ResultRow.singleRow(Singletons.require(HostService.class).getGlobalVariable(ssCon.getCatalogDAO(), variableName)));
	}

}

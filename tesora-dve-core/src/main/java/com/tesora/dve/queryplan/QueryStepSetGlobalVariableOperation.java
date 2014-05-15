// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepSetGlobalVariableOperation extends QueryStepOperation {
	
	String variableName;
	String value;

	public QueryStepSetGlobalVariableOperation(String variableName, String value) {
		super();
		this.variableName = variableName;
		this.value = value;
	}

	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
        Singletons.require(HostService.class).setGlobalVariable(ssCon.getCatalogDAO(), variableName, value);
	}

}

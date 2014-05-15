// OS_STATUS: public
package com.tesora.dve.queryplan;

import java.sql.Types;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.WorkerGroup;


public class QueryStepGetSessionVariableOperation extends QueryStepOperation {
	
	String variableName;
	private String alias;

	public QueryStepGetSessionVariableOperation(String variableName, String alias) {
		super();
		this.variableName = variableName;
		this.alias = alias;
	}
	
	public QueryStepGetSessionVariableOperation(String variableName) {
		this(variableName, variableName);
	}

	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
		resultConsumer.inject(ColumnSet.singleColumn(alias, Types.VARCHAR), 
				ResultRow.singleRow(ssCon.getSessionVariable(variableName)));
	}

}

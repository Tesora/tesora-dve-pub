// OS_STATUS: public
package com.tesora.dve.queryplan;


import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepUpdateAllOperation extends QueryStepDMLOperation {

	static Logger logger = Logger.getLogger( QueryStepSelectAllOperation.class );
	
	DistributionModel distributionModel;
	SQLCommand command;

	public QueryStepUpdateAllOperation(PersistentDatabase execCtxDBName, DistributionModel distModelOfTable, SQLCommand command) throws PEException {
		super(execCtxDBName);
		if (command.isEmpty())
			throw new PEException("Cannot create QueryStep with empty SQL command");

		this.distributionModel = distModelOfTable;
		this.command = command;
	}

	public QueryStepUpdateAllOperation(PersistentDatabase execCtxDBName, DistributionModel distModelOfTable, String command) throws PEException {
		this(execCtxDBName, distModelOfTable, new SQLCommand(command));
	}
	/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		WorkerExecuteRequest req = new WorkerExecuteRequest(ssCon.getTransactionalContext(), command).onDatabase(database);
		resultConsumer.setRowAdjuster(distributionModel.getUpdateAdjuster());
		beginExecution();
		wg.execute(StaticDistributionModel.SINGLETON.mapForQuery(wg, command), req, resultConsumer);
		endExecution(resultConsumer.getUpdateCount());
	}
	
	@Override
	public String describeForLog() {
		StringBuilder buf = new StringBuilder();
		String rawSQL = command.getRawSQL();
		if (command.getStatementType() == StatementType.INSERT)
			rawSQL = rawSQL.substring(0,1024);
		buf.append("UpdateAll stmt=").append(rawSQL);
		return buf.toString();
	}
	
}

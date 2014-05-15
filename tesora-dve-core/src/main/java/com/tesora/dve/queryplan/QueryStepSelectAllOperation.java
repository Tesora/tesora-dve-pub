// OS_STATUS: public
package com.tesora.dve.queryplan;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.worker.MysqlParallelResultConsumer;
import com.tesora.dve.worker.WorkerGroup;

/**
 * {@link QueryStepSelectAllOperation} is a <b>QueryStep</b> operation which
 * executes a sql query against a {@link PersistentGroup}, returning the results
 * of the query.
 *
 */
@XmlType(name="QueryStepSelectAllOperation")
public class QueryStepSelectAllOperation extends QueryStepResultsOperation {

	static Logger logger = Logger.getLogger( QueryStepSelectAllOperation.class );
	
	@XmlElement(name="DistributionModel")
	DistributionModel distributionModel;
	@XmlElement(name="SqlCommand")
	SQLCommand command;
	
	/**
	 * Constructs a <b>QueryStepSelectAllOperation</b> to execute the provided 
	 * sql query against the database specified by <em>execCtxDBName</em>.
	 * 
	 * @param execCtxDBName database in which the <em>command</em> will be executed
	 * @param command sql query to execute
	 * @throws PEException 
	 */
	public QueryStepSelectAllOperation(PersistentDatabase execCtxDBName, DistributionModel distModel, SQLCommand command) throws PEException {
		super(execCtxDBName);
		if (command.isEmpty())
			throw new PEException("Cannot create QueryStep with empty SQL command");

		this.distributionModel= distModel;
		this.command = command;
	}

	public QueryStepSelectAllOperation(PersistentDatabase execCtxDBName, DistributionModel distModel, String command) throws PEException {
		this(execCtxDBName, distModel, new SQLCommand(command));
	}

		/**
	 * Called by <b>QueryStep</b> to execute the query.
	 * @throws Throwable 
	 */
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		resultConsumer.setResultsLimit(getResultsLimit());
		WorkerExecuteRequest req = new WorkerExecuteRequest(ssCon.getTransactionalContext(), command).onDatabase(database);
		beginExecution();
		wg.execute(distributionModel.mapForQuery(wg, command), req, resultConsumer);
		if (resultConsumer instanceof MysqlParallelResultConsumer) 
			endExecution(((MysqlParallelResultConsumer)resultConsumer).getNumRowsAffected());
	}

	@Override
	public boolean requiresTransaction() {
		return false;
	}
	
	@Override
	public String describeForLog() {
		StringBuilder buf = new StringBuilder();
		buf.append("SelectAll stmt=").append(command.getRawSQL());
		return buf.toString();
	}
}

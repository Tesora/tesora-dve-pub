// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteKillRequest;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.worker.WorkerGroup;

public class KillStatement extends SessionStatement {

	private final int connectionId;
	private final boolean isKillConnection;

	public KillStatement(int connectionId, boolean isKillConnection) {
		super("KILL " + (isKillConnection ? "CONNECTION" : "QUERY"));
		this.connectionId = connectionId;
		this.isKillConnection = isKillConnection;
	}

	public boolean isPassthrough() {
		return false;
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {

		if (null == PerHostConnectionManager.INSTANCE.getConnectionInfo(connectionId))
			throw new PESQLException("Unknown thread id: " + connectionId);

		es.append(new TransientSessionExecutionStep(null, buildAllNonUniqueSitesGroup(sc), getAdhocSQL(), false, true,
				new AdhocOperation() {

					@Override
					public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
							throws Throwable {

						WorkerExecuteRequest req = new WorkerExecuteKillRequest(ssCon.getNonTransactionalContext(),
								new SQLCommand("KILL QUERY"), connectionId);
						wg.execute(WorkerGroup.MappingSolution.AllWorkers, req, resultConsumer);

						if (isKillConnection)
							PerHostConnectionManager.INSTANCE.closeConnection(connectionId);
					}
				}));
	}

}

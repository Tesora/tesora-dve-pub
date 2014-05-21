package com.tesora.dve.sql.statement.session;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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

package com.tesora.dve.server.messaging;

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

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.db.GroupDispatch;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.server.connectionmanager.ConnectionInfo;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.worker.Worker;

public class WorkerExecuteKillRequest extends WorkerExecuteRequest {

	private static final long serialVersionUID = 1L;

	private final int connectionId;

	public WorkerExecuteKillRequest(SSContext ssContext, SQLCommand command, int connectionId) {
		super(ssContext, command);
        this.connectionId = connectionId;
	}

	@Override
	protected void executeStatement(Worker w, SQLCommand stmtCommand, GroupDispatch resultConsumer, CompletionHandle<Boolean> callersResults) {

		ConnectionInfo connectionInfo = PerHostConnectionManager.INSTANCE.getConnectionInfo(connectionId);
		if (connectionInfo == null) {
            PECodingException peCodingException = new PECodingException("Unknown thread id: " + connectionId);
            peCodingException.fillInStackTrace();
            callersResults.failure(peCodingException);
            return;
        }

		int siteConnId = connectionInfo.getSiteConnectionId(w.getWorkerSite());
		if (siteConnId == 0){
			// no corresponding thread on this site
			callersResults.success(true);
            return;
        }

		StringBuilder sql = new StringBuilder(stmtCommand.getRawSQL());
		sql.append(' ').append(siteConnId);
		super.executeStatement(w, new SQLCommand(sql.toString()), resultConsumer, callersResults);
	}

}

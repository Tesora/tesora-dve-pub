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
import com.tesora.dve.db.DBNative;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.Worker;

public class WorkerDropDatabaseRequest extends WorkerRequest {
	private static final long serialVersionUID = 1L;

	final String databaseName;
	
	public WorkerDropDatabaseRequest(SSContext ssContext, String databaseName) {
		super(ssContext);
		this.databaseName = databaseName;
	}

	@Override
	public void executeRequest(final Worker w, CompletionHandle<Boolean> promise) {

		String localizedDBName = UserDatabase.getNameOnSite(databaseName, w.getWorkerSite());
		SQLCommand ddl = Singletons.require(DBNative.class)
				.getDropDatabaseStmt(PerHostConnectionManager.INSTANCE.lookupConnection(this.getConnectionId()), localizedDBName);
        this.execute(w, ddl, promise);
	}

    @Override
	public String toString() {
		return new StringBuffer().append("WorkerCreateDatabaseRequest("+databaseName+")").toString();
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.W_CREATE_DB_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}


	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return new LogSiteStatisticRequest(OperationClass.DDL);
	}
}

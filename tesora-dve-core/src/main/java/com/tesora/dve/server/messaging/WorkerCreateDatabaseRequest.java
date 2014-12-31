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

import com.tesora.dve.db.DBNative;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.worker.Worker;

public class WorkerCreateDatabaseRequest extends WorkerRequest {	
	static Logger logger = Logger.getLogger( WorkerCreateDatabaseRequest.class );

	private static final long serialVersionUID = 1L;

	private final PersistentDatabase newDatabase;
	private final boolean ifNotExists;
	private final String defaultCharSet;
	private final String defaultCollation;
	
	public WorkerCreateDatabaseRequest(SSContext ctx, PersistentDatabase newDatabase, boolean ifNotExists) {
		super(ctx);
		this.newDatabase = newDatabase;
		this.ifNotExists = ifNotExists;
		this.defaultCharSet = newDatabase.getDefaultCharacterSetName();
		this.defaultCollation = newDatabase.getDefaultCollationName();
	}

	@Override
	public void executeRequest(final Worker w, CompletionHandle<Boolean> promise) {

		final String onSiteName = newDatabase.getNameOnSite(w.getWorkerSite());

        final SQLCommand ddl = Singletons
                .require(DBNative.class)
                .getCreateDatabaseStmt(PerHostConnectionManager.INSTANCE.lookupConnection(this.getConnectionId()), onSiteName, ifNotExists, defaultCharSet,
						defaultCollation);
        
		this.execute(w, ddl, promise);
	}

	@Override
	public String toString() {
		return new StringBuffer().append("WorkerCreateDatabaseRequest("+ newDatabase.getUserVisibleName() +")").toString();
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

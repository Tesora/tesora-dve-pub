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
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.Worker;

public class WorkerAlterDatabaseRequest extends WorkerRequest {

	private static final long serialVersionUID = -1638470828106328178L;
	private static final Logger logger = Logger.getLogger(WorkerAlterDatabaseRequest.class);

	private final UserDatabase alteredDatabase;

	public WorkerAlterDatabaseRequest(final SSContext ctx, final UserDatabase alteredDatabase) {
		super(ctx);
		this.alteredDatabase = alteredDatabase;
	}

	@Override
	public void executeRequest(final Worker w, final DBResultConsumer resultConsumer, CompletionHandle<Boolean> promise) {

		final String onSiteName = this.alteredDatabase.getNameOnSite(w.getWorkerSite());

		final String defaultCharSet = alteredDatabase.getDefaultCharacterSetName();
		final String defaultCollation = alteredDatabase.getDefaultCollationName();

        final SQLCommand ddl = Singletons.require(HostService.class).getDBNative().getAlterDatabaseStmt(onSiteName, defaultCharSet, defaultCollation);
		simpleExecute(w,resultConsumer,ddl,promise);
	}

	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return new LogSiteStatisticRequest(OperationClass.DDL);
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.W_ALTER_DB_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

	@Override
	public String toString() {
		final String thisClassName = this.getClass().getSimpleName();
		return thisClassName.concat("(").concat(this.alteredDatabase.getName()).concat(")");
	}

}

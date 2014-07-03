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

import java.sql.SQLException;

import javax.transaction.xa.XAException;

import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.comms.client.messages.ExecuteResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerStatement;

public class WorkerGrantPrivilegesRequest extends WorkerRequest {

	static Logger logger = Logger.getLogger( WorkerCreateDatabaseRequest.class );

	private static final long serialVersionUID = 1L;

	final String userDeclaration;
	final String databaseName;
	
	public WorkerGrantPrivilegesRequest(SSContext ssContext, String onDB, String userDeclaration) {
		super(ssContext);
		this.databaseName = onDB;
		this.userDeclaration = userDeclaration;
	}
	
	@Override
	public ResponseMessage executeRequest(Worker w, DBResultConsumer resultConsumer) throws SQLException, PEException, XAException {
		WorkerStatement stmt = w.getStatement();
		// String localizedDBName = UserDatabase.getNameOnSite(databaseName, w.getWorkerSite());
		// having an issue with temp sites and privileges, so who cares about security
		// String ddl = Host.getDBNative().getGrantprivilegesCommand(userDeclaration, localizedDBName);
        SQLCommand ddl = Singletons.require(HostService.class).getDBNative().getGrantPriviledgesCommand(userDeclaration, "*");
		if (logger.isDebugEnabled()) {
			logger.debug(w.getName()+": Current database is "+ w.getCurrentDatabaseName());
			logger.debug(w.getName()+":executing statement " + ddl);
		}
        try {
            PEDefaultPromise<Boolean> promise = new PEDefaultPromise<>();
            stmt.execute(getConnectionId(), ddl, resultConsumer,promise);
            promise.sync();
        } catch (Exception e) {
            throw new PEException(e);
        }
		
		return new ExecuteResponse(false, resultConsumer.getUpdateCount(), null).from(w.getAddress()).success();
	}

	@Override
	public String toString() {
		return new StringBuffer().append("WorkerGrantPrivilegesRequest(" + databaseName + ".* to " + userDeclaration + ")").toString();
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.W_GRANT_PRIVILEDGES_REQUEST;
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

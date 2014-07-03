package com.tesora.dve.queryplan;

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
import java.util.Collection;

import javax.transaction.xa.XAException;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.comms.client.messages.ExecuteResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.worker.Worker;

public class WorkerMultiInsertRequest extends WorkerExecuteRequest {

	private static final long serialVersionUID = 1L;
	
	private final MultiMap<StorageSite, SQLCommand> mappedInserts;

	public WorkerMultiInsertRequest(SSContext ctx, MultiMap<StorageSite, SQLCommand> mappedInserts) {
		super(ctx, SQLCommand.EMPTY);
		this.mappedInserts = mappedInserts;
	}

	@Override
	public void executeRequest(Worker w, DBResultConsumer resultConsumer) throws SQLException, PEException, XAException {

		Collection<SQLCommand> cmds = mappedInserts.get(w.getWorkerSite());

		if ( cmds != null ) {
			for (SQLCommand sqlCommand : cmds) {
				executeStatement(w, sqlCommand, resultConsumer);
			}
		}
		
		new ExecuteResponse(resultConsumer.hasResults(), resultConsumer.getUpdateCount(), null ).from(w.getAddress()).success();
	}

	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return new LogSiteStatisticRequest(OperationClass.EXECUTE);
	}

	@Override
	public MessageType getMessageType() {
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}


}

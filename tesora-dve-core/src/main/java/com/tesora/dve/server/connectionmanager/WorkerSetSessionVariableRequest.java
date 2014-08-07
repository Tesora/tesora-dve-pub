package com.tesora.dve.server.connectionmanager;

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

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.mysql.SetVariableSQLBuilder;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.Worker;

import java.util.Map;

public class WorkerSetSessionVariableRequest extends WorkerExecuteRequest {

	public static final long serialVersionUID = 1L;

    final Map<String,String> currentSessionVariables;
    final SetVariableSQLBuilder sqlBuilder;

    public WorkerSetSessionVariableRequest(SSContext ssContext, Map<String,String> sessionVariables, SetVariableSQLBuilder sqlBuilder) {
        super(ssContext, SQLCommand.EMPTY);
        this.currentSessionVariables = sessionVariables;
        this.sqlBuilder = sqlBuilder;
    }

    @Override
    public void executeRequest(Worker w, DBResultConsumer resultConsumer, CompletionHandle<Boolean> promise) {
        w.updateSessionVariables(currentSessionVariables,sqlBuilder,promise);
    }

    @Override
	public MessageType getMessageType() {
		return MessageType.SET_SESSION_VAR;
	}

	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return null;
	}

}

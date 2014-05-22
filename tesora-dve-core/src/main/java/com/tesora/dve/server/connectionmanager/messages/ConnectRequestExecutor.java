package com.tesora.dve.server.connectionmanager.messages;

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

import org.apache.log4j.Logger;

import com.tesora.dve.comms.client.messages.ConnectRequest;
import com.tesora.dve.comms.client.messages.ConnectResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.UserCredentials;

public class ConnectRequestExecutor implements AgentExecutor<SSConnection> {
	private static Logger logger = Logger.getLogger(ConnectRequestExecutor.class);

	@Override
	public ResponseMessage execute(SSConnection connMgr, Object message) throws Throwable {
		ConnectRequest cr = (ConnectRequest) message;
		UserCredentials userCred = new UserCredentials(cr.getUserID(), cr.getPassword(), cr.getIsPlaintext());

		try {
			connMgr.startConnection(userCred);
		} catch (PEException pe) {
			PerHostConnectionManager.INSTANCE.addConnectFailure();
			throw pe;
		}

		if (logger.isDebugEnabled())
			logger.debug("ConnectRequest/c(" + connMgr.getName() + "): connected with " + userCred.toString());

		return new ConnectResponse(connMgr.getName()).success();
	}
}

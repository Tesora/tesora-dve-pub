// OS_STATUS: public
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

import com.tesora.dve.comms.client.messages.MysqlPreConnectResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.mysql.MysqlNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.db.mysql.common.MysqlHandshake;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class PreConnectRequestExecutor implements AgentExecutor<SSConnection> {

	@Override
	public ResponseMessage execute(SSConnection connMgr, Object message) throws PEException {
		ResponseMessage response;

		switch (connMgr.getDBNative().getDbType()) {
		case MYSQL:
		case MARIADB:
			MysqlNative myNative = (MysqlNative) connMgr.getDBNative();
			MysqlHandshake myHandshake = connMgr.getHandshake();

			MysqlPreConnectResponse myResp = new MysqlPreConnectResponse();
			myResp.setDBType(myNative.getDbType());
			myResp.setCharset(myHandshake.getServerCharSet());
			myResp.setConnectId(connMgr.getConnectionId());
			myResp.setPluginData(myHandshake.getPluginData());
			myResp.setSalt(myHandshake.getSalt());
			myResp.setServerCapabilities(myHandshake.getServerCapabilities());
			myResp.setServerVersion(myHandshake.getServerVersion());
			myResp.success();
			response = (ResponseMessage) myResp;
			break;
		default:
			throw new PEException("Database type " + connMgr.getDBNative().getDbType() + " is unknown.");
		}
		return response;
	}
}

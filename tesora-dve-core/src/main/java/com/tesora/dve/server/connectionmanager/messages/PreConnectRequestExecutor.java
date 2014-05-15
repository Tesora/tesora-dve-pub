// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

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

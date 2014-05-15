// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import com.tesora.dve.comms.client.messages.GetDatabaseResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.Database;

public class GetDatabaseRequestExecutor implements AgentExecutor<SSConnection> {

	@Override
	public ResponseMessage execute(SSConnection connMgr, Object message) throws Throwable {
		String udb_name = null;
		Database<?> db = connMgr.getCurrentDatabase();
		if (db != null)
			udb_name = db.getUserVisibleName();
		return new GetDatabaseResponse(udb_name).success();
	}
}

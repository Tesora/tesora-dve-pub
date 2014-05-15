// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import com.tesora.dve.comms.client.messages.DBMetadataResponse;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;

public class DBMetadataRequestExecutor implements AgentExecutor<SSConnection> {

	@Override
	public ResponseMessage execute(SSConnection connMgr, Object message) throws Throwable {
        return new DBMetadataResponse(Singletons.require(HostService.class).getDBNative().getDBMetadata()).success();
	}
}

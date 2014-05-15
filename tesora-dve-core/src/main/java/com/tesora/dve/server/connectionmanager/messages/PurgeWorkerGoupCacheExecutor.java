// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.groupmanager.PurgeWorkerGroupCaches;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class PurgeWorkerGoupCacheExecutor implements AgentExecutor<SSConnection> {
	
	public final static PurgeWorkerGoupCacheExecutor INSTANCE = new PurgeWorkerGoupCacheExecutor();

	@Override
	public ResponseMessage execute(SSConnection ssCon, Object message) throws Throwable {
		PurgeWorkerGroupCaches purgeRequest = (PurgeWorkerGroupCaches) message;
		ssCon.clearWorkerGroupCache(purgeRequest.getGroup());
		return null;
	}

}

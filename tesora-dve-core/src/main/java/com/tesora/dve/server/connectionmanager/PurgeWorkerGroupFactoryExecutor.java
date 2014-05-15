// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.groupmanager.PurgeWorkerGroupCaches;
import com.tesora.dve.server.connectionmanager.messages.AgentExecutor;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public class PurgeWorkerGroupFactoryExecutor implements AgentExecutor<BroadcastMessageAgent> {
	
	public final static PurgeWorkerGroupFactoryExecutor INSTANCE = new PurgeWorkerGroupFactoryExecutor();

	@Override
	public ResponseMessage execute(BroadcastMessageAgent agent, Object message) throws Throwable {
		PurgeWorkerGroupCaches purgeRequest = (PurgeWorkerGroupCaches) message;
		WorkerGroupFactory.clearGroupFromCache(agent, purgeRequest.getGroup());
		return null;
	}

}

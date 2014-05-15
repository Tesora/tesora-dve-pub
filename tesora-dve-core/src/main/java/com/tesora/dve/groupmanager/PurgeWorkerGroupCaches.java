// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.worker.agent.Agent;

public class PurgeWorkerGroupCaches extends GroupMessage {

	static Logger logger = Logger.getLogger( PurgeWorkerGroupCaches.class );

	private static final long serialVersionUID = 1L;
	
	PersistentGroup group;

	public PurgeWorkerGroupCaches(PersistentGroup group) {
		this.group = group;
	}

	public PersistentGroup getGroup() {
		return group;
	}

	@Override
	void execute(HostService hostService) {
		try {
            Agent.dispatch(Singletons.require(HostService.class).getBroadcastMessageAgentAddress(), this);
			PerHostConnectionManager.INSTANCE.sendToAllConnections(this);
		} catch (PEException e) {
			logger.warn(e);
		}
	}

	@Override
	public MessageType getMessageType() {
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		return null;
	}

}

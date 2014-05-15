// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.server.global.HostService;

public class GroupMessageSync extends GroupMessage {
	
	private static final long serialVersionUID = 1L;

	PEDefaultPromise<Void> completePromise = new PEDefaultPromise<Void>();

	@Override
	void execute(HostService hostService) {
		completePromise.success(null);
	}
	
	public void sync() throws Exception {
		completePromise.sync();
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

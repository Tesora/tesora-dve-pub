package com.tesora.dve.groupmanager;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.server.global.HostService;

public class GlobalVariableInvalidationMessage extends GroupMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5131350016956586338L;

	@Override
	void execute(HostService hostService) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MessageType getMessageType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageVersion getVersion() {
		// TODO Auto-generated method stub
		return null;
	}

}

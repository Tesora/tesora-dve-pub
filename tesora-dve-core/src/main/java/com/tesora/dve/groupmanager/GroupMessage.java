// OS_STATUS: public
package com.tesora.dve.groupmanager;

import com.tesora.dve.comms.client.messages.ClientMessage;
import com.tesora.dve.server.global.HostService;

@SuppressWarnings("serial")
public abstract class GroupMessage extends ClientMessage {
	
	abstract void execute(HostService hostService);

}

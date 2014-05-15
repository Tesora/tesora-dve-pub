// OS_STATUS: public
package com.tesora.dve.server.connectionmanager.messages;

import com.tesora.dve.comms.client.messages.ResponseMessage;


public interface AgentExecutor<AgentType> {

	ResponseMessage execute(AgentType agent, Object message) throws Throwable;

}

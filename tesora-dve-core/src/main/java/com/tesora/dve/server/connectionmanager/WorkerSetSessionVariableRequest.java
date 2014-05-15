// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.singleton.Singletons;

public class WorkerSetSessionVariableRequest extends WorkerExecuteRequest {

	public static final long serialVersionUID = 1L;
	
	public WorkerSetSessionVariableRequest(SSContext ssContext, String assignmentClause) {
        super(ssContext, new SQLCommand(Singletons.require(HostService.class).getDBNative().getSetSessionVariableStatement(assignmentClause)));
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.SET_SESSION_VAR;
	}

	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return null;
	}

}

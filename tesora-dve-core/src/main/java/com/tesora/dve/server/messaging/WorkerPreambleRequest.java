// OS_STATUS: public
package com.tesora.dve.server.messaging;

import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;

public class WorkerPreambleRequest extends WorkerExecuteRequest {
	private static final long serialVersionUID = 1L;

	public WorkerPreambleRequest(SSContext ssContext, SQLCommand command) {
		super(ssContext, command);
	}

	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return null;
	}

}

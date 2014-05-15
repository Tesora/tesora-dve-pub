// OS_STATUS: public
package com.tesora.dve.server.statistics.manager;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.server.statistics.StatisticsResponse;


public class LogGlobalStatisticRequest extends StatisticsRequest {
	
	private static final long serialVersionUID = 1L;

	private int updateCount;
	private int responseTime;

	public LogGlobalStatisticRequest(long updateCount, long responseTime) {		this.updateCount = (int) updateCount;
		this.responseTime = (int) responseTime;
	}

	@Override
	public StatisticsResponse executeRequest(StatisticsManager sm) {
		if (updateCount > 0)
			sm.logGlobalUpdate(responseTime);
		else
			sm.logGlobalQuery(responseTime);
		return null;
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

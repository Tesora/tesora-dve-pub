// OS_STATUS: public
package com.tesora.dve.server.statistics.manager;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.server.statistics.StatisticsResponse;

public class GetStatisticsRequest extends StatisticsRequest {

	private static final long serialVersionUID = 1L;

	@Override
	public StatisticsResponse executeRequest(StatisticsManager statisticsManager) {
		return new GetStatisticsResponse(statisticsManager.getStatistics());
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.STAT_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

}

// OS_STATUS: public
package com.tesora.dve.server.statistics.manager;

import com.tesora.dve.comms.client.messages.RequestMessage;
import com.tesora.dve.server.statistics.StatisticsResponse;

@SuppressWarnings("serial")
public abstract class StatisticsRequest extends RequestMessage {
	
	public abstract StatisticsResponse executeRequest(StatisticsManager statisticsManager);

}

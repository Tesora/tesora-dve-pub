// OS_STATUS: public
package com.tesora.dve.server.statistics.manager;

import com.tesora.dve.server.statistics.ServerStatistics;
import com.tesora.dve.server.statistics.StatisticsResponse;

public class GetStatisticsResponse extends StatisticsResponse {

	private static final long serialVersionUID = 1L;
	private ServerStatistics stats;

	public GetStatisticsResponse() {
	}
	
	public GetStatisticsResponse(ServerStatistics stats) {
		this.stats = stats;
	}

	public ServerStatistics getStats() {
		return stats;
	}

}

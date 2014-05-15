// OS_STATUS: public
package com.tesora.dve.server.statistics.manager;

import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.server.statistics.SiteStatKey.SiteType;
import com.tesora.dve.server.statistics.StatisticsResponse;


public class LogSiteStatisticRequest extends StatisticsRequest {

	static final long serialVersionUID = 1L;
	
	OperationClass opClass;
	int responseTime;
	String siteName;
	SiteType siteType;

	int recordsAffected = 1;

	public LogSiteStatisticRequest(OperationClass opClass) {
		super();
		this.opClass = opClass;
	}

	@Override
	public StatisticsResponse executeRequest(StatisticsManager sm) {
		sm.logSiteStatistic(siteType, siteName, opClass, responseTime / recordsAffected);
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

	public void setExecutionDetails(String name, int execTime) {
		this.siteName = name;
		this.responseTime = execTime;
	}

	public int getRecordsAffected() {
		return recordsAffected;
	}

	public void setRecordsAffected(int recordsAffected) {
		this.recordsAffected = recordsAffected;
	}

	public void setSiteDetails(String name, SiteType type) {
		siteName = name;
		siteType = type;
	}

}

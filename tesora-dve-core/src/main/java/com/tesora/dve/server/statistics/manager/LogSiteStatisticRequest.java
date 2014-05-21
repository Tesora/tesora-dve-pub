// OS_STATUS: public
package com.tesora.dve.server.statistics.manager;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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

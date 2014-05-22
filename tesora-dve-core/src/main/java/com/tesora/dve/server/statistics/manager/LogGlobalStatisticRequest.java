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

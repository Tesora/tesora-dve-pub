// OS_STATUS: public
package com.tesora.dve.server.statistics;

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

import com.tesora.dve.server.statistics.TimingValue;

public class CumulativeStatisticsAccumulator implements StatisticsAccumulator {

	long cumulativeResponseTime;
	long count;
	
	@Override
	public void addDatum(int responseTimeMS) {
		cumulativeResponseTime += (responseTimeMS == 0) ? 1 : responseTimeMS;
		count++;
	}

	@Override
	public float getAverageTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getAverageValue() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getTransactionsPerSecond() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isDebugMode() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setDebugMode(boolean debugMode) {
		// TODO Auto-generated method stub

	}

	@Override
	public TimingValue getTimingValue() {
		return new TimingValue(cumulativeResponseTime, count);
	}

}

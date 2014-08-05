package com.tesora.dve.variable.status;

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

import com.tesora.dve.sql.PlannerStatisticType;
import com.tesora.dve.sql.PlannerStatistics;

public class PlannerStatisticsVariableHandler extends EnumStatusVariableHandler<PlannerStatisticType> { 

	public PlannerStatisticsVariableHandler(String name,
			PlannerStatisticType counter) {
		super(name, counter);
	}

	@Override
	protected long getCounterValue(PlannerStatisticType counter)
			throws Throwable {
		return PlannerStatistics.getCurrentValue(counter);
	}

	@Override
	protected void resetCounterValue(PlannerStatisticType counter)
			throws Throwable {
		PlannerStatistics.resetCurrentValue(counter);
	}
}

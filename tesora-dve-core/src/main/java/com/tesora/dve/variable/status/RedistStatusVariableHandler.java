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

import com.tesora.dve.common.catalog.StorageGroup.GroupScale;
import com.tesora.dve.worker.DynamicGroup;

public class RedistStatusVariableHandler extends EnumStatusVariableHandler<GroupScale> {

	public RedistStatusVariableHandler(String name, GroupScale scale) {
		super(name,scale);
	}
	
	@Override
	protected long getCounterValue(GroupScale counter) throws Throwable {
		return DynamicGroup.getCurrentUsage(counter);
	}

	@Override
	protected void resetCounterValue(GroupScale counter) throws Throwable {
		DynamicGroup.resetCurrentUsage(counter);
	}
}

// OS_STATUS: public
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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.StorageGroup.GroupScale;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.worker.DynamicGroup;

public class RedistStatusVariableHandler extends StatusVariableHandler {

	@Override
	public String getValue(CatalogDAO c, String name) throws PENotFoundException {
		try {
			return Long.toString(DynamicGroup.getCurrentUsage(GroupScale.valueOf(defaultValue)));
		} catch (PEException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}

	@Override
	public void reset(CatalogDAO c, String name) throws PENotFoundException {
		try {
			DynamicGroup.resetCurrentUsage(GroupScale.valueOf(defaultValue));
		} catch (PEException e) {
			throw new PENotFoundException("Unable to reset status variable " + name, e);
		}
	}
}

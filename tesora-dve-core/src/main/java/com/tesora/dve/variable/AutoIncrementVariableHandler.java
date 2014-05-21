// OS_STATUS: public
package com.tesora.dve.variable;

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

import java.io.Serializable;

import com.tesora.dve.common.catalog.AutoIncrementTracker;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;

public class AutoIncrementVariableHandler extends ConfigVariableHandler
		implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final String AUTO_INCREMENT_MIN_BLOCK_SIZE = "auto_increment_min_block_size";
	public static final String AUTO_INCREMENT_MAX_BLOCK_SIZE = "auto_increment_max_block_size";
	public static final String AUTO_INCREMENT_PREFETCH_THRESHOLD = "auto_increment_prefetch_threshold";

	@Override
	public void setValue(CatalogDAO c, String name, String value)
			throws PEException {

		int intValue = VariableValueConverter.toInternalInteger(value);

		if (AUTO_INCREMENT_PREFETCH_THRESHOLD.equalsIgnoreCase(name)) {
			if (intValue < 0 || intValue > 100)
				throw new PEException("Invalid value for " + name + " (must be in range 1 - 100)"); 
		} else if (AUTO_INCREMENT_MIN_BLOCK_SIZE.equalsIgnoreCase(name)) {
			if (intValue < 1)
				throw new PEException("Invalid value for " + name + " (must be greater than 1)"); 
		}

		super.setValue(c, name, value);
	}

	@Override
	public void onValueChange(String name, String newValue) throws PEException {
		int intValue = VariableValueConverter.toInternalInteger(newValue);
		if (AUTO_INCREMENT_MIN_BLOCK_SIZE.equalsIgnoreCase(name))
			AutoIncrementTracker.setMinBlockSize(intValue);
		else if (AUTO_INCREMENT_MAX_BLOCK_SIZE.equalsIgnoreCase(name))
			AutoIncrementTracker.setMaxBlockSize(intValue);
		else if (AUTO_INCREMENT_PREFETCH_THRESHOLD.equalsIgnoreCase(name))
			AutoIncrementTracker.setPrefetchThreshold(intValue);
	}

}

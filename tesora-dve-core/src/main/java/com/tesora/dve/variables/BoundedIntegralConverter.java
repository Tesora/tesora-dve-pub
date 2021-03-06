package com.tesora.dve.variables;

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

import com.tesora.dve.exceptions.PEException;

public class BoundedIntegralConverter extends IntegralValueConverter {

	// use null to denote not to check
	private final Long minimum;
	private final Long maximum;
	
	public BoundedIntegralConverter(Long min, Long max) {
		this.minimum = min;
		this.maximum = max;
	}
	
	@Override
	public Long convertToInternal(String varName, String in) throws PEException {
		final Long number = super.convertToInternal(varName, in);

		// Out-of-range values should be replaced by the nearest limit.
		if (number != null) {
			if ((minimum != null) && (number < minimum)) {
				return minimum;
			} else if ((maximum != null) && (number > maximum)) {
				return maximum;
			}
		}

		return number;
	}
	
}

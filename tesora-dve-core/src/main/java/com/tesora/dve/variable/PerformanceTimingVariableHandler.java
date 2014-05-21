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

import com.tesora.dve.clock.TimingServiceConfiguration;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.singleton.Singletons;

public class PerformanceTimingVariableHandler extends ConfigVariableHandler {

	@Override
	public void setValue(CatalogDAO c, String name, String value) throws PEException {
		VariableValueConverter.toInternalBoolean(value);
		super.setValue(c, name, value);
	}

	@Override
	public void onValueChange(String variableName, String newValue) throws PEException {
		boolean enable = VariableValueConverter.toInternalBoolean(newValue);
        TimingServiceConfiguration timingConfiguration = Singletons.lookup(TimingServiceConfiguration.class);
        if (timingConfiguration != null)
            timingConfiguration.setTimingEnabled(enable);
	}
}

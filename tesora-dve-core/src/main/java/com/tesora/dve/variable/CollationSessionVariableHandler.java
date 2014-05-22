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

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public class CollationSessionVariableHandler extends DBSessionVariableHandler {

	public static final String VARIABLE_NAME = "collation_connection";

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		if (VARIABLE_NAME.equalsIgnoreCase(name)) {
			Singletons.require(HostService.class).getDBNative().assertValidCollation(value);
			super.setValue(ssCon, name, value);
		} else {
			throw new PECodingException(this.getClass().getSimpleName() + " called for " + name + " which it cannot handle");
		}
	}
}

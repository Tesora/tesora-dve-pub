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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

/**
 * Allows the declaration of a variable with a default value that is returned.
 * Throws exception if setValue is called.
 */
public class LiteralSessionVariableHandler extends SessionVariableHandler {

	@Override
	public void setValue(SSConnection ssCon, String name, String value) throws PEException {
		throw new PEException("Variable '" + name + "' not settable as session variable");
	}

	@Override
	public String getValue(SSConnection ssCon, String name) throws PEException {
		return getDefaultValue(ssCon);
	}

}

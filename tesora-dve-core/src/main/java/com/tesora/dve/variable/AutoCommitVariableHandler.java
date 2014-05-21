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

import java.sql.Types;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class AutoCommitVariableHandler extends SessionVariableHandler {

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		ssCon.setAutoCommitMode(this.toBoolean(value));
		super.setValue(ssCon, name, value);
	}

    @Override
    public String getSessionAssignmentClause(String name, String value) {
        return "autocommit = 1";
    }

	@Override
	public ResultCollector getValueAsResult(SSConnection ssConnection,
			String variableName) throws PEException {
		return ResultCollectorFactory.getInstance(Types.INTEGER, getValue(ssConnection, variableName));
	}
	
	private boolean toBoolean(final String value) throws PEException {
		if (value != null) {
			if (value.equalsIgnoreCase("1") || value.equalsIgnoreCase("ON")) {
				return true;
			} else if (value.equalsIgnoreCase("0") || value.equalsIgnoreCase("OFF")) {
				return false;
			}
		}
		
		throw new PEException("Invalid value given for the AUTOCOMMIT variable.");
	}

}

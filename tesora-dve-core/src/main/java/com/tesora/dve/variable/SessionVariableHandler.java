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

import java.sql.Types;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class SessionVariableHandler extends VariableHandler {
	
	public static final String TRANSACTION_ISOLATION_LEVEL = "tx_isolation";
	public static final String WAIT_TIMEOUT = "wait_timeout";
	
	String variableName;
	String defaultValue;
	
	public void initialise(String variableName, String defaultValue) {
		this.variableName = variableName;
		this.defaultValue = defaultValue;
	}
	
	public String getVariableName() {
		return variableName;
	}

	public void setValue(SSConnection ssCon, String name, String value) throws PEException {
		ssCon.setSessionVariableValue(name, value);
	}

	public String getValue(SSConnection ssCon, String name) throws PEException {
		return ssCon.getSessionVariableValue(name);
	}
	
	public String getSessionAssignmentClause(String name, String value) {
		return null;
	}

	public ResultCollector getValueAsResult(SSConnection ssConnection,
			String variableName) throws PEException {
		return ResultCollectorFactory.getInstance(Types.VARCHAR, getValue(ssConnection, variableName));
	}

	public String getDefaultValue(SSConnection ssCon) throws PEException {
		return defaultValue;
	}
	
	public boolean defaultValueRequiresConnection() {
		return false;
	}
}

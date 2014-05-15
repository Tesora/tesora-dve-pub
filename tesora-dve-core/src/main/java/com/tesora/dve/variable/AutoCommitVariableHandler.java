// OS_STATUS: public
package com.tesora.dve.variable;

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

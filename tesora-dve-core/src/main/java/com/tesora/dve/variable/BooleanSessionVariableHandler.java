// OS_STATUS: public
package com.tesora.dve.variable;

import java.sql.Types;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class BooleanSessionVariableHandler extends SessionVariableHandler {

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		VariableValueConverter.toInternalBoolean(value);
		super.setValue(ssCon, name, value);
	}

	@Override
	public ResultCollector getValueAsResult(SSConnection ssConnection,
			String variableName) throws PEException {
		return ResultCollectorFactory.getInstance(Types.BOOLEAN, getValue(ssConnection, variableName));
	}
}

// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class SessionBeanHandler extends SessionVariableHandler {

	String beanName;

	@Override
	public void initialise(String name, String defaultValue) {
		beanName = defaultValue;
	}

	@Override
	public void setValue(SSConnection ssCon, String name, String value)
			throws PEException {
		throw new PEException("setValue not supported for session variable " + name);
	}

	@Override
	public String getValue(SSConnection ssCon, String name) throws PEException {
		return BeanVariableHandler.callGetValue(ssCon, beanName);
	}
}

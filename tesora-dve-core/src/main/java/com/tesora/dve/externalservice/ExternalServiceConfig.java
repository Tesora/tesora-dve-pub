// OS_STATUS: public
package com.tesora.dve.externalservice;

import com.tesora.dve.exceptions.PEException;

public interface ExternalServiceConfig {

	public void unmarshall(String config) throws PEException;
	public String marshall();
}
// OS_STATUS: public
package com.tesora.dve.variable.status;

import java.sql.Types;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;
import com.tesora.dve.variable.VariableHandler;


public abstract class StatusVariableHandler extends VariableHandler {

	String defaultValue;
	String variableName;
	
	public void initialise(String name, String defaultValue) {
		this.defaultValue = defaultValue;
		this.variableName = name;
	}

	public abstract String getValue(CatalogDAO c, String name) throws PEException;

	public abstract void reset(CatalogDAO c, String name) throws PEException;
	
	public ResultCollector getValueAsResult(CatalogDAO catalogDAO, String variableName) throws PEException {
		return ResultCollectorFactory.getInstance(Types.VARCHAR, getValue(catalogDAO, variableName));
	}
}

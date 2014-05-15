// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;

public class DebugContextVariableHandler extends ConfigVariableHandler {

	@Override
	public void setValue(CatalogDAO c, String name, String value) throws PEException {
		VariableValueConverter.toInternalBoolean(value);
		super.setValue(c, name, value);
	}

	@Override
	public void onValueChange(String variableName, String newValue) throws PEException {
		boolean enable = VariableValueConverter.toInternalBoolean(newValue);
		PEThreadContext.setEnabled(enable);
	}
}

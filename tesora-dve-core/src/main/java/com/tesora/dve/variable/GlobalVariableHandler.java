// OS_STATUS: public
package com.tesora.dve.variable;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;

public abstract class GlobalVariableHandler extends VariableHandler {
	
	public abstract void setValue(CatalogDAO c, String name, String value) throws PEException;
	
	public abstract String getValue(CatalogDAO c, String name) throws PEException;
}

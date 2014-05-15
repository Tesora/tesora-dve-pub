// OS_STATUS: public
package com.tesora.dve.upgrade;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

// implementors know the version they represent, and how to upgrade from the previous version.
public interface CatalogVersion {

	abstract public int getSchemaVersion();
	
	abstract public boolean hasInfoSchemaUpgrade();

	abstract public void upgrade(DBHelper helper) throws PEException;
	
}

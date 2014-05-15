// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

public class InfoSchemaUpgradeVersion extends SimpleCatalogVersion {

	public InfoSchemaUpgradeVersion(int v) {
		super(v,true);
	}
	
	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {};
	}

}

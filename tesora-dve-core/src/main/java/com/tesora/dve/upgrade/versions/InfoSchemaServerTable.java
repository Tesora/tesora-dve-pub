// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

public class InfoSchemaServerTable extends InfoSchemaUpgradeVersion {

	public InfoSchemaServerTable(int version) {
		super(version);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {};
	}

}

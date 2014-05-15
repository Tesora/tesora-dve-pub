// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import java.util.Arrays;
import java.util.Collection;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

public class AdaptiveAutoIncrementVersion extends SimpleCatalogVersion {

	private static String[] obsoleteVariables = new String[] { "auto_increment_block_size" };

	public AdaptiveAutoIncrementVersion(int v) {
		super(v, false);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[0];
	}

	@Override
	public Collection<String> getObsoleteVariables() {
		return Arrays.asList(obsoleteVariables);
	}

}

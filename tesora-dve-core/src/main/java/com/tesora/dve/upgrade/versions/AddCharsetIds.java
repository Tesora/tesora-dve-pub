// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

public class AddCharsetIds extends SimpleCatalogVersion {

	public AddCharsetIds(int version) {
		super(version);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {
				"update character_sets set id = 8 where character_set_name = 'latin1'",
				"update character_sets set id = 11 where character_set_name = 'ascii'",
				"update character_sets set id = 33 where character_set_name = 'utf8'"
		};
	}

}

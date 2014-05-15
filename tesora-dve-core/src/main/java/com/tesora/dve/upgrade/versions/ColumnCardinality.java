// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

public class ColumnCardinality extends SimpleCatalogVersion {

	public ColumnCardinality(int v) {
		super(v, true);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {
				"alter table user_key drop column card",
				"alter table user_key_column add column cardinality bigint"
		};
	}

}

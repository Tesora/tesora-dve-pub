// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import java.util.Arrays;
import java.util.Collection;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

public class AddTemplateMode extends SimpleCatalogVersion {

	public AddTemplateMode(int v) {
		super(v, true);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {
				"alter table user_database add column template_mode varchar(255) after templatereqd",
				"update user_database set template_mode = 'OPTIONAL' where (template IS NULL)",
				"update user_database set template_mode = 'REQUIRED' where ((template IS NOT NULL) AND (templatereqd = '0'))",
				"update user_database set template_mode = 'STRICT' where (templatereqd = '1')",
				"alter table user_database drop column templatereqd"
		};
	}

	@Override
	public Collection<String> getObsoleteVariables() {
		return Arrays.asList("template_optional");
	}

}

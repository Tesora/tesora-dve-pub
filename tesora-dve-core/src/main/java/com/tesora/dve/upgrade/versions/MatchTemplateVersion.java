// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

public class MatchTemplateVersion extends SimpleCatalogVersion {

	public MatchTemplateVersion(int v) {
		super(v, true);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {
				"alter table template add column `template_comment` varchar(255) after `template_id`",
				"alter table template add column `dbmatch` varchar(255) after `template_comment`"
		};
	}

}

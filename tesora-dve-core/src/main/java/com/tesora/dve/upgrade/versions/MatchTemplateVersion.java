// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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

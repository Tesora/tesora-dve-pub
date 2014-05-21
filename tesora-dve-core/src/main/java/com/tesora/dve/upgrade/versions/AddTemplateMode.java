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

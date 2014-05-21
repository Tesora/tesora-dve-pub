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

public class RawPlans extends SimpleCatalogVersion {

	public RawPlans(int v) {
		super(v, true);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {
			"create table rawplan (plan_id integer not null auto_increment, cachekey longtext not null, plan_comment varchar(255), definition longtext not null, enabled integer not null, name varchar(255) not null, user_database_id integer not null, primary key (plan_id)) ENGINE=InnoDB;",
			"alter table rawplan add index FK3ACDEF51F229CB7C (user_database_id), add constraint FK3ACDEF51F229CB7C foreign key (user_database_id) references user_database (user_database_id);"	
		};
	}

}

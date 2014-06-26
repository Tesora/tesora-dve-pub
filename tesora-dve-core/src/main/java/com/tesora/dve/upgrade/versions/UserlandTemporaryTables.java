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

public class UserlandTemporaryTables extends SimpleCatalogVersion {

	public UserlandTemporaryTables(int v) {
		super(v);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {
				"create table user_temp_table (id integer not null auto_increment, db varchar(255) not null, table_engine varchar(255) not null, name varchar(255) not null, server_id varchar(255) not null, session_id integer not null, primary key (id)) ENGINE=InnoDB"
		};
	}

}

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

public class TriggersVersion extends SimpleCatalogVersion {

	public TriggersVersion(int v) {
		super(v,true);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {
				"create table user_trigger ("
						+"trigger_id integer not null auto_increment, "
						+"character_set_client varchar(255) not null, "
						+"collation_connection varchar(255) not null, "
						+"database_collation varchar(255) not null, "
						+"trigger_body longtext not null, "
						+"trigger_event varchar(255) not null, "
						+"trigger_name varchar(255) not null, "
						+"origsql longtext not null, "
						+"sql_mode longtext not null, "
						+"trigger_time varchar(255) not null, "
						+"user_id integer not null, "
						+"table_id integer not null, "
						+"primary key (trigger_id)) ENGINE=InnoDB",
				"alter table user_trigger add index fk_trigger_user (user_id), add constraint fk_trigger_user foreign key (user_id) references user (id)",
				"alter table user_trigger add index fk_trigger_table_def (table_id), add constraint fk_trigger_table_def foreign key (table_id) references user_table (table_id)"				
		};
	}

}

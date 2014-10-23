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

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.InformationCallback;
import com.tesora.dve.exceptions.PEException;

public class MTForeignKeys extends ComplexCatalogVersion {

	public MTForeignKeys(int v) {
		super(v,true);
	}

	private static String[] beforeSQL = new String[] {
		"alter table scope drop foreign key FK6833E5490459BB5",
		"alter table scope drop column scope_future_table_id",
		"alter table user_key add column `physical_constraint_name` varchar(255)",
		"alter table user_key add column `hidden` integer",
		"alter table user_table drop foreign key FK7358465AE7F25E6B",
		"alter table user_table drop column refs, drop column privtab_ten_id"
	};
	
	private static String[] afterSQL = new String[] {
		"alter table user_key change column `hidden` `hidden` integer not null",
	};
	
	private static String[] obsoleteVariables = new String[] {
		"adaptive_tickler_interval",
		"adaptive_tickler_count",
		"adaptive_share_limit",
		"adaptive_shape_concurrency_limit"
	};
	
	@Override
	public void upgrade(DBHelper helper, InformationCallback ic) throws PEException {
		clearInfoSchema(helper);

		for(String s : beforeSQL)
			execQuery(helper,s);
		
		execQuery(helper, "update user_key set physical_constraint_name = constraint_name");
		execQuery(helper, "update user_key set hidden = 0");
		
		for(String s : afterSQL)
			execQuery(helper,s);
		
		dropVariables(helper,Arrays.asList(obsoleteVariables));
	}

}

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

import java.sql.ResultSet;
import java.util.HashMap;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.upgrade.CatalogStateValidator;
import com.tesora.dve.upgrade.CatalogVersions.CatalogVersionNumber;

public class GlobalVariablesValidator extends CatalogStateValidator {

	public GlobalVariablesValidator() {
		super(CatalogVersionNumber.GLOBAL_VARIABLES);
	}

	@Override
	protected String[] getPopulation() {
		return new String[] {
				"insert into dynamic_policy (policy_id, aggregate_class, aggregate_count, aggregate_provider," 
						+"large_class, large_count, large_provider, medium_class, medium_count, medium_provider, "
						+"name, small_class, small_count, small_provider, strict) values "
						+"(1,'POOL1',1,'op','POOL1',3,'op','POOL1',2,'op','DefaultPolicy','POOL1',1,'op',0)",
				"insert into config (config_id, name, value) values (52,'sql_mode','NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'), "
				+"(53,'template_mode','REQUIRED'),(29,'innodb_lock_wait_timeout','3'), (17,'default_time_zone','SYSTEM')",
				"insert into persistent_group (persistent_group_id, name) values (3,'DefaultGroup')",
				"insert into project (project_id, name, default_policy_id, default_persistent_group_id) values "
				+"(1,'Default',1,3)",
		};
	}
	
	@Override
	public String validate(DBHelper helper) throws Throwable {
		// the structural validation will determine whether we got the structure wrong
		// so we just need to make sure that the varconfig table contains sql_mode, template_mode, innodb_lock_wait_timeout,
		// persistent_group, default_policy.
		ResultSet rs = null;
		HashMap<String,String> expected = new HashMap<String,String>();
		expected.put("sql_mode","NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
		expected.put("template_mode","REQUIRED");
		expected.put("innodb_lock_wait_timeout","3");
		expected.put("persistent_group","DefaultGroup");
		expected.put("dynamic_policy","DefaultPolicy");
		expected.put("time_zone","+00:00");
		
		try {
			if (helper.executeQuery("select name, value from varconfig where name in ('sql_mode','template_mode','innodb_lock_wait_timeout','persistent_group','dynamic_policy', 'time_zone')")) {
				rs = helper.getResultSet();
				while(rs.next()) {
					String name = rs.getString(1);
					String value = rs.getString(2);
					String eval = expected.remove(name);
					if (eval == null)
						return "Expected to find a value for " + name + " but do not have one";
					if (!eval.equals(value)) 
						return String.format("Expected value '%s' for '%s' but found '%s'",eval,name,value);
				}
			}
			if (!expected.isEmpty())
				return "Did not migrate all variables, missing: " + expected.keySet();
		} finally {
			if (rs != null)
				rs.close();
		}
		// TODO Auto-generated method stub
		return null;
	}

}

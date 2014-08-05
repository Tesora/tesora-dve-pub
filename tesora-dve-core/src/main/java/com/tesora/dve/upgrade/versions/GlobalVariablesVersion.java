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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.variables.VariableManager;
import com.tesora.dve.variables.VariableOption;

public class GlobalVariablesVersion extends ComplexCatalogVersion {

	public GlobalVariablesVersion(int version) {
		super(version, true);
		// TODO Auto-generated constructor stub
	}

	// our list of changes:
	// project.default_policy_id has been removed.  any default policy should go on the variable.
	// project.default_persistent_group_id has been removed.  any default pg should go on the variable.
	// the fks from project to persistent_group, dynamic_policy have been removed:
	//    fk_project_def_sg, fk_project_def_policy
	// added the varconfig table.
	// remove the config table.
	
	// I believe what we should do is:
	// create the varconfig table
	// associate each row of config with one of the variables.  build sess vars for any unknowns.
	//   don't forget that we renamed a couple of variables (default_time_zone, etc.)
	//   also, look up the default_dynamic_policy, default_persistent_group values, associate those.
	// load the varconfig table using variables + config.
	// drop the config table
	// drop the fks on project
	// alter project, remove the two columns.
	
	private static final String varconfigDefinition = 
			"create table varconfig (id integer not null auto_increment, "
			+"description varchar(255), name varchar(255) not null, options varchar(255) not null, "
			+"scopes varchar(255) not null, "
			+"value varchar(255), value_type varchar(255) not null, primary key (id), unique(name)) engine=innodb";
	
	private static final String[] postSQL = new String[] {
		"drop table config",
		"alter table project drop foreign key fk_project_def_sg",
		"alter table project drop foreign key fk_project_def_policy",
		"alter table project drop column default_policy_id",
		"alter table project drop column default_persistent_group_id"
	};
			
	
	@Override
	public void upgrade(DBHelper helper) throws PEException {
		// TODO Auto-generated method stub
		// can't really rely on the HostService here
		VariableManager vm = VariableManager.getManager();
		
		try {				
			helper.executeQuery(varconfigDefinition);
		} catch (SQLException sqle) {
			throw new PEException("Unable to create new variable table");
		}
		
		Map<String,String> persistentValues = buildPersistentValues(helper);
		loadVariableDefinitions(helper,vm,persistentValues);

		for(String s : postSQL) try {
			helper.executeQuery(s);
		} catch (SQLException sqle) {
			throw new PEException("Unable to execute '" + s + "'",sqle);
		}
		
	}
	
	private Map<String,String> buildPersistentValues(DBHelper helper) throws PEException {
		HashMap<String,String> persistentValues = new HashMap<String,String>();
		
		try {
			ResultSet rs = null;
			try {
				if (helper.executeQuery("select name, value from config")) {
					rs = helper.getResultSet();
					while(rs.next()) {
						String name = rs.getString(1);
						String value = rs.getString(2);
						if (value == null) value = VariableHandler.NULL_VALUE;
						persistentValues.put(VariableManager.normalize(name),value);						
					}
				}
			} finally {
				if (rs != null)
					rs.close();
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to obtain current config values",sqle);
		}
		// also, load the pers group and dyn policy values
		try {
			Long pgid = null;
			Long dpid = null;
			ResultSet rs = null;
			try {
				if (helper.executeQuery("select default_persistent_group_id, default_policy_id from project")) {
					rs = helper.getResultSet();
					if (rs.next()) {
						pgid = rs.getLong(1);
						dpid = rs.getLong(2);
					}
				}
			} finally {
				if (rs != null)
					rs.close();
			}
			if (pgid != null) {
				String name = (String) getSingleField(helper,String.format("select name from persistent_group where persistent_group_id = %d",pgid));
				persistentValues.put(VariableManager.normalize(KnownVariables.PERSISTENT_GROUP.getName()),name);
			}
			if (dpid != null) {
				String name= (String) getSingleField(helper,String.format("select name from dynamic_policy where policy_id = %d",dpid));
				persistentValues.put(VariableManager.normalize(KnownVariables.DYNAMIC_POLICY.getName()),name);
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to obtain current values for dynamic_policy, persistent_group",sqle);
		}
		
		
		return persistentValues;
	}
	
	private void loadVariableDefinitions(DBHelper helper, VariableManager vm, Map<String,String> persistentValues) throws PEException {
		try {
			helper.prepare("insert into varconfig (options, description, name, scopes, value, value_type) values (?,?,?,?,?,?)");
		} catch (SQLException sqle) {
			throw new PEException("Unable to prepare varconfig insert",sqle);
		}
		
		for(VariableHandler vh : vm.getAllHandlers()) {
			String pValue = persistentValues.remove(VariableManager.normalize(vh.getName()));
			Object converted = null;
			if (pValue == null) {
				// not configured, check to see if this is one of the special cases
				if (vh == KnownVariables.TIME_ZONE) {
					pValue = persistentValues.remove(VariableManager.normalize("default_time_zone"));
				} 
			} else {
				converted = vh.toInternal(pValue);
			}
			if (converted == null)
				converted = vh.getDefaultOnMissing();

			List<Object> params = new ArrayList<Object>();
			params.add(VariableHandler.convertOptions(vh.getOptions()));
			params.add(vh.getDescription());
			params.add(vh.getName());
			params.add(VariableHandler.convertScopes(vh.getScopes()));
			params.add(vh.toRow(converted));
			params.add(vh.getMetadata().getTypeName());
			try {
				helper.executePrepared(params);
			} catch (SQLException sqle) {
				throw new PEException("Unable to insert varconfig for " + vh.getName(),sqle);
			}
		}
		// if there is anything left in persistentValues, treat it as an emulated session variable with string values.
		EnumSet<VariableScopeKind> sessionScope = EnumSet.of(VariableScopeKind.SESSION);
		EnumSet<VariableOption> options = EnumSet.of(VariableOption.EMULATED,VariableOption.PASSTHROUGH);
		for(Map.Entry<String, String> me : persistentValues.entrySet()) {
			List<Object> params = new ArrayList<Object>();
			params.add(VariableHandler.convertOptions(options));
			params.add(null);
			params.add(me.getKey());
			params.add(VariableHandler.convertScopes(sessionScope));
			params.add(me.getValue());
			params.add("varchar");
			try {
				helper.executePrepared(params);
			} catch (SQLException sqle) {
				throw new PEException("Unable to insert varconfig for " + me.getKey());
			}
		}
	}
}

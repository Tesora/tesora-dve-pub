package com.tesora.dve.upgrade;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.sql.util.Functional;

public class TableState {

	public static final SchemaStateQuery columnBuilder =
			new SchemaStateQuery("select column_name, column_default, is_nullable, column_key, column_type "
					+"from information_schema.columns where table_schema = ':schema' and table_name = ':table'",
					1);
	public static final SchemaStateQuery constraintsBuilder =
			new SchemaStateQuery("select constraint_name, table_name, constraint_type "
					+"from information_schema.table_constraints where table_schema = ':schema' and table_name = ':table'",
					1);
	public static final SchemaStateQuery fkConstraintBuilder =
			new SchemaStateQuery("select constraint_name, referenced_table_name, unique_constraint_name, update_rule, delete_rule "
					+"from information_schema.referential_constraints where constraint_schema = ':schema' and table_name = ':table'",
					1);
	public static final SchemaStateQuery keysBuilder = 
			new SchemaStateQuery("show keys in :table",
					new int [] { 3, 4} ,new int[] { 7 });
	
	private StateItemSet[] features;
	
	public TableState(DBHelper helper, String schemaName, String tableName) throws Throwable {
		Map<String,String> params = new HashMap<String,String>();
		params.put(":schema", schemaName);
		params.put(":table", tableName);
		features = new StateItemSet[] {
			new KeyStateItemSet("column").build(columnBuilder, helper, params),
			new BlobStateItemSet("constraint").build(constraintsBuilder, helper, params),
			new BlobStateItemSet("foreign key constraint").build(fkConstraintBuilder, helper, params),
			new BlobStateItemSet("key").build(keysBuilder, helper, params)
		};
	}
	
	public String differs(TableState other) {
		List<String> messages = new ArrayList<String>();
		for(int i = 0; i < features.length; i++)
			features[i].collectDifferences(other.features[i], messages);
		if (messages.isEmpty()) return null;
		return Functional.join(messages, System.getProperty("line.separator"));
	}
		
	public boolean isEmpty() {
		for(StateItemSet sis : features) {
			if (!sis.isEmpty())
				return false;
		}
		return true;
	}
	
}

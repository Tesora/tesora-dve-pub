// OS_STATUS: public
package com.tesora.dve.sql.infoschema.persist;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.SimpleColumnMetadata;
import com.tesora.dve.persist.SimpleTableMetadata;

public class CatalogSchema {

	SimpleTableMetadata userTable;
	SimpleTableMetadata userColumn;
	SimpleTableMetadata userDatabase;
	
	public CatalogSchema() throws PEException {
		userDatabase = buildUserDatabase();
		userTable = buildUserTable();
		userColumn = buildUserColumn();
	}
	
	public SimpleTableMetadata getTable() { return userTable; }
	public SimpleTableMetadata getColumn() { return userColumn; }
	public SimpleTableMetadata getDatabase() { return userDatabase; }
		
	private SimpleTableMetadata buildUserTable() throws PEException {
		SimpleTableMetadata t = new SimpleTableMetadata("user_table");
		t.addColumn(new SimpleColumnMetadata("table_id",true));
		t.addColumn(new SimpleColumnMetadata("name"));
		t.addColumn(new SimpleColumnMetadata("state"));
		t.addColumn(new SimpleColumnMetadata("distribution_model_id"));
		t.addColumn(new SimpleColumnMetadata("persistent_group_id"));
		t.addColumn(new SimpleColumnMetadata("user_database_id",userDatabase.getColumn("user_database_id")));
		t.addColumn(new SimpleColumnMetadata("engine"));
		t.addColumn(new SimpleColumnMetadata("table_type"));
		return t;
	}
	
	private SimpleTableMetadata buildUserColumn() throws PEException {
		SimpleTableMetadata t = new SimpleTableMetadata("user_column");
		t.addColumn(new SimpleColumnMetadata("user_column_id",true));
		t.addColumn(new SimpleColumnMetadata("auto_generated"));
		t.addColumn(new SimpleColumnMetadata("data_type"));
		t.addColumn(new SimpleColumnMetadata("default_value_is_constant"));
		t.addColumn(new SimpleColumnMetadata("has_default_value"));
		t.addColumn(new SimpleColumnMetadata("name"));
		t.addColumn(new SimpleColumnMetadata("native_type_name"));
		t.addColumn(new SimpleColumnMetadata("nullable"));
		t.addColumn(new SimpleColumnMetadata("on_update"));
		t.addColumn(new SimpleColumnMetadata("order_in_table"));
		t.addColumn(new SimpleColumnMetadata("prec"));
		t.addColumn(new SimpleColumnMetadata("scale"));
		t.addColumn(new SimpleColumnMetadata("size"));
		t.addColumn(new SimpleColumnMetadata("cdv"));
		t.addColumn(new SimpleColumnMetadata("hash_position"));
		t.addColumn(new SimpleColumnMetadata("user_table_id",userTable.getColumn("table_id")));
		t.addColumn(new SimpleColumnMetadata("charset"));
		t.addColumn(new SimpleColumnMetadata("collation"));
		t.addColumn(new SimpleColumnMetadata("native_type_modifiers"));
		return t;
	}
	
	private SimpleTableMetadata buildUserDatabase() throws PEException {
		SimpleTableMetadata t = new SimpleTableMetadata("user_database");
		t.addColumn(new SimpleColumnMetadata("user_database_id",true));
		t.addColumn(new SimpleColumnMetadata("default_character_set_name"));
		t.addColumn(new SimpleColumnMetadata("default_collation_name"));
		t.addColumn(new SimpleColumnMetadata("multitenant_mode"));
		t.addColumn(new SimpleColumnMetadata("name"));
		t.addColumn(new SimpleColumnMetadata("default_group_id"));
		t.addColumn(new SimpleColumnMetadata("fk_mode"));
		return t;
	}
}

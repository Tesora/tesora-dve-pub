package com.tesora.dve.persist;

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
import java.util.List;

import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;

public class PersistedInsert {

	SimpleTableMetadata table;
	List<SimpleColumnMetadata> columns = new ArrayList<SimpleColumnMetadata>();
	List<Object> values = new ArrayList<Object>();
	
	public PersistedInsert(SimpleTableMetadata tab) {
		table = tab;
	}
	
	public void add(SimpleColumnMetadata column, Object value) {
		columns.add(column);
		values.add(value);
	}
	
	public String getSQL() {
		StringBuilder buf = new StringBuilder();
		buf.append("INSERT INTO `").append(table.getName()).append("` (");
		Functional.join(columns, buf, ", ", new BinaryProcedure<SimpleColumnMetadata,StringBuilder>() {

			@Override
			public void execute(SimpleColumnMetadata aobj, StringBuilder bobj) {
				bobj.append("`").append(aobj.getName()).append("`");
			}
			
		});
		buf.append(") VALUES (");
		Functional.join(values, buf, ", ", new BinaryProcedure<Object,StringBuilder>() {

			@Override
			public void execute(Object aobj, StringBuilder bobj) {
				if (aobj == null)
					bobj.append("NULL");
				else if (aobj instanceof String)
					bobj.append("'").append(aobj).append("'");
				else
					bobj.append(aobj);
			}
			
		});
		buf.append(")");
		return buf.toString();
	}
	
}

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

import java.util.LinkedHashMap;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.util.Functional;

// going for really simple here too
public class SimpleTableMetadata {

	LinkedHashMap<String,SimpleColumnMetadata> columns = new LinkedHashMap<String, SimpleColumnMetadata>();
	String tableName;
	
	public SimpleTableMetadata(String n) {
		tableName = n;
	}
	
	public String getName() {
		return tableName;
	}
	
	public void addColumn(SimpleColumnMetadata scm) throws PEException {
		SimpleColumnMetadata e = columns.get(scm.getName());
		if (e != null)
			throw new PEException("Column name " + e.getName() + " already exists in table " + tableName);
		columns.put(scm.getName(), scm);
		scm.setTable(this);
	}
	
	public SimpleColumnMetadata getColumn(String name) throws PEException {
		return getColumn(name,true);
	}
	
	public SimpleColumnMetadata getColumn(String name, boolean mustExist) throws PEException {
		SimpleColumnMetadata scm = columns.get(name);
		if (scm == null && mustExist)
			throw new PEException("Unknown column in simple table " + tableName + ": " + name);
		return scm;
	}
	
	public List<SimpleColumnMetadata> getColumnOrder() {
		return Functional.toList(columns.values());
	}
	
	public SimpleColumnMetadata getGeneratedId() {
		for(SimpleColumnMetadata scm : columns.values())
			if (scm.isGenerated())
				return scm;
		return null;
	}
}

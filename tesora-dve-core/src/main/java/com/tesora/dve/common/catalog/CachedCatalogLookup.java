// OS_STATUS: public
package com.tesora.dve.common.catalog;

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


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PENotFoundException;

public class CachedCatalogLookup<TableClass extends CatalogEntity> {
	
	protected Map<Object, Integer> valueLookup = new HashMap<Object, Integer>();
	protected Class<? extends CatalogEntity> tableClass;
	protected String columnName;
	String tableName;
	
	public CachedCatalogLookup(Class<? extends CatalogEntity> tableClass, String columnName) {
		this.tableClass = tableClass;
		this.columnName = columnName;
		this.tableName = tableClass.getSimpleName();
	}

	public TableClass findByValue(CatalogDAO c, Object value, Object param, boolean exceptionOnNotFound) throws PENotFoundException {
		TableClass instance = getByValue(c, value, exceptionOnNotFound);
		if (instance == null)
			instance = queryByValue(c, value, param, exceptionOnNotFound);
		return instance;		
	}

	@SuppressWarnings("unchecked")
	public TableClass queryByValue(CatalogDAO c, Object value, Object param, boolean exceptionOnNotFound) throws PENotFoundException {
		TableClass instance;
		String queryString = "from " + tableName + " where " + columnName + " = :value";
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("value", param);
		List<CatalogEntity> res = c.queryCatalogEntity(queryString, params);
		instance = (TableClass) CatalogDAO.onlyOne(res, tableName, value.toString(), exceptionOnNotFound);
		if (instance != null)
			valueLookup.put(value, instance.getId());
		return instance;
	}

	public TableClass getByValue(CatalogDAO c, Object value, boolean exceptionOnNotFound) throws PENotFoundException {
		TableClass instance = null;
		if (valueLookup.containsKey(value)) {
			Integer instanceId = valueLookup.get(value);
			instance = c.findByKey(tableClass, instanceId);
			if (instance == null && exceptionOnNotFound)
				throw new PENotFoundException(tableName + " instance for " + columnName + "=" + value + " has been deleted");
		}
		return instance;
	}
	
	public TableClass findByValue(CatalogDAO c, Object value, boolean exceptionOnNotFound) throws PENotFoundException {
		return findByValue(c,value,value,exceptionOnNotFound);
	}

	public <T extends CatalogEntity> void putByValue(Object value, T param) {
		valueLookup.put(value, param.getId());
	}
}

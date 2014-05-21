// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical.catalog;

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


import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class CatalogInformationSchemaTable extends LogicalInformationSchemaTable {

	// hibernate entity class name
	protected Class<?> entityClass;
	protected String entityName;
	protected String tableName;
	
	protected InfoSchemaTable config;
	
	public CatalogInformationSchemaTable(Class<?> entKlass, InfoSchemaTable anno, String catTabName) {
		super(new UnqualifiedName(anno.logicalName()));
		entityClass = entKlass;
		entityName = entKlass.getSimpleName();
		tableName = catTabName;
		config = anno;
	}

	@Override
	public String toString() {
		return "CatalogInformationSchemaTable{logicalName=" + getName().getSQL() + ", entity=" + entityName + ", table=" + tableName + "}";
	}

	@Override
	public Class<?> getEntityClass() {
		return entityClass;
	}
	
	@Override
	public String getEntityName() {
		return entityName;
	}

	@Override
	public String getTableName() {
		return tableName;
	}	
}

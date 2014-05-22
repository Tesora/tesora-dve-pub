package com.tesora.dve.sql.infoschema.annos;

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

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.DatabaseView.DatabaseViewCacheKey;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.util.UnaryFunction;

public enum InfoView {

	INFORMATION(PEConstants.INFORMATION_SCHEMA_DBNAME,InformationSchemaTableView.regularNameFunc),
	SHOW(PEConstants.SHOW_SCHEMA_DBNAME,InformationSchemaTableView.regularNameFunc),
	MYSQL(PEConstants.MYSQL_SCHEMA_DBNAME,InformationSchemaTableView.regularNameFunc);
	
	private final String persistentDBName;
	private final UnaryFunction<Name[], AbstractInformationSchemaColumnView> nameFunc;
	private final DatabaseViewCacheKey cacheKey;
	
	private InfoView(String pdbn, UnaryFunction<Name[], AbstractInformationSchemaColumnView> nf) {
		persistentDBName = pdbn;
		nameFunc = nf;
		cacheKey = new DatabaseViewCacheKey(pdbn);
	}
	
	public String getUserDatabaseName() {
		return persistentDBName;
	}
	
	public UnaryFunction<Name[], AbstractInformationSchemaColumnView> getNameFunction() {
		return nameFunc;
	}
	
	public DatabaseViewCacheKey getCacheKey() {
		return cacheKey;
	}
	
}

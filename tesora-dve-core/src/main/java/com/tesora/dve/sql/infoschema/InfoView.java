package com.tesora.dve.sql.infoschema;

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
import com.tesora.dve.sql.infoschema.InformationSchemaDatabase.DatabaseViewCacheKey;

public enum InfoView {

	INFORMATION(PEConstants.INFORMATION_SCHEMA_DBNAME,false,true),
	SHOW(PEConstants.SHOW_SCHEMA_DBNAME,false,false),
	MYSQL(PEConstants.MYSQL_SCHEMA_DBNAME,true,false);
	
	private final String persistentDBName;
	private final DatabaseViewCacheKey cacheKey;
	private final boolean lookupCaseSensitive;
	private final boolean capitalizeNames;
	
	private InfoView(String pdbn, 
			boolean caseSensitive,
			boolean capitalizeNames) {
		persistentDBName = pdbn;
		cacheKey = new DatabaseViewCacheKey(pdbn);
		this.lookupCaseSensitive = caseSensitive;
		this.capitalizeNames = capitalizeNames;
	}
	
	public String getUserDatabaseName() {
		return persistentDBName;
	}
	
	public DatabaseViewCacheKey getCacheKey() {
		return cacheKey;
	}
	
	public boolean isLookupCaseSensitive() {
		return lookupCaseSensitive;
	}
	
	public boolean isCapitalizeNames() {
		return capitalizeNames;
	}
}

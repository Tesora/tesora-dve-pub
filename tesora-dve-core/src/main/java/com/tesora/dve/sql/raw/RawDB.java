// OS_STATUS: public
package com.tesora.dve.sql.raw;

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

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.Schema;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;

public class RawDB implements Database<PEAbstractTable<?>> {

	private final PEDatabase base;
	private final RawSchema schema;
	
	private final RawDBCacheKey ck;
	
	public RawDB(PEDatabase pdb) {
		base = pdb;
		schema = new RawSchema(this);
		ck = new RawDBCacheKey(this);
	}
	
	PEDatabase getBaseDatabase() {
		return base;
	}
	
	@Override
	public Name getName() {
		return base.getName();
	}

	@Override
	public String getNameOnSite(StorageSite site) {
		return base.getNameOnSite(site);
	}

	@Override
	public int getId() {
		return 0;
	}

	@Override
	public String getUserVisibleName() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public SchemaCacheKey getCacheKey() {
		return ck;
	}

	@Override
	public Schema<PEAbstractTable<?>> getSchema() {
		return schema;
	}

	@Override
	public UserDatabase getPersistent(SchemaContext sc) {
		return base.getPersistent(sc);
	}

	@Override
	public PEPersistentGroup getDefaultStorage(SchemaContext sc) {
		return base.getDefaultStorage(sc);
	}

	@Override
	public SchemaEdge<PEPersistentGroup> getDefaultStorageEdge() {
		return base.getDefaultStorageEdge();
	}

	@Override
	public boolean isInfoSchema() {
		return true;
	}

	private static class RawDBCacheKey extends SchemaCacheKey<RawDB> {

		private static final long serialVersionUID = 1L;
		private final RawDB theDB;
		
		public RawDBCacheKey(RawDB rdb) {
			theDB = rdb;
		}
		
		@Override
		public int hashCode() {
			return theDB.getBaseDatabase().getCacheKey().hashCode();
		}

		@Override
		public boolean equals(Object o) {
			return theDB.equals(o);
		}

		@Override
		public RawDB load(SchemaContext sc) {
			return theDB;
		}

		@Override
		public String toString() {
			return null;
		}
		
	}

	@Override
	public String getDefaultCollationName() {
		return null;
	}

	@Override
	public String getDefaultCharacterSetName() {
		return null;
	}
	
}

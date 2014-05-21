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
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.Schema;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;

public class DatabaseView implements Database<InformationSchemaTableView> {

	private final SchemaView schema;
	private final UnqualifiedName name;
	private final int id;
	
	public DatabaseView(SchemaContext sc, UserDatabase udb, SchemaView schema) {
		this.schema = schema;
		this.name = new UnqualifiedName(udb.getName());
		this.id = udb.getId();
	}

	public static boolean isInfoSchemaName(Name n) {
		String raw = n.getUnqualified().get().toLowerCase();
		return PEConstants.INFORMATION_SCHEMA_DBNAME.equals(raw);
	}
	
	public static boolean isInformationSchemaDatabase(UserDatabase udb) {
		return PEConstants.INFORMATION_SCHEMA_DBNAME.equals(udb.getName());
	}
	
	public static boolean isInformationSchemaDatabase(SchemaContext sc, Database<?> db) {
		UserDatabase udb = db.getPersistent(sc);
		return isInformationSchemaDatabase(udb);
	}
	
	@Override
	public Name getName() {
		return name;
	}

	@Override
	public Schema<InformationSchemaTableView> getSchema() {
		return schema;
	}

	@Override
	public UserDatabase getPersistent(SchemaContext sc) {
		return sc.getCatalog().getDAO().findByKey(UserDatabase.class, id);
	}

	@Override
	public PEPersistentGroup getDefaultStorage(SchemaContext sc) {
		// no default storage
		return null;
	}

	@Override
	public boolean isInfoSchema() {
		return true;
	}

	@Override
	public String getNameOnSite(StorageSite site) {
		return UserDatabase.getNameOnSite(name.get(), site);
	}

	@Override
	public String getUserVisibleName() {
		return name.get();
	}

	@Override
	public int getId() {
		return 0;
	}

	@Override
	public SchemaCacheKey<?> getCacheKey() {
		return new DatabaseViewCacheKey(name);
	}
	
	@SuppressWarnings("rawtypes")
	public static class DatabaseViewCacheKey extends SchemaCacheKey {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String name;
		
		public DatabaseViewCacheKey(Name n) {
			this(n.getUnquotedName().getUnqualified().get());
		}
		
		public DatabaseViewCacheKey(String n) {
			super();
			name = n;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof DatabaseViewCacheKey) {
				DatabaseViewCacheKey dvk = (DatabaseViewCacheKey) o;
				return name.equals(dvk.name);
			}
			return false;
		}

		@Override
		public Object load(SchemaContext sc) {
			UserDatabase udb = sc.getCatalog().findUserDatabase(name);
			if (udb == null)
				return null;
            Database<?> peds = Singletons.require(HostService.class).getInformationSchema().buildPEDatabase(sc, udb);
			if (peds == null)
				throw new IllegalStateException("Unable to load info schema view");
			return peds;
		}

		@SuppressWarnings("unchecked")
		@Override
		public int hashCode() {
			return initHash(Database.class, name.hashCode());
		}
		
		@Override
		public String toString() {
			return "DatabaseView:" + name;
		}
		
	}

	@Override
	public SchemaEdge<PEPersistentGroup> getDefaultStorageEdge() {
		return null;
	}

	@Override
	public String getDefaultCollationName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDefaultCharacterSetName() {
		// TODO Auto-generated method stub
		return null;
	}
	
}

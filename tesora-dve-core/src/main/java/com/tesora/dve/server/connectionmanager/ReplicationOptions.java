package com.tesora.dve.server.connectionmanager;

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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.util.ListSet;

public class ReplicationOptions {

	Logger logger = Logger.getLogger(ReplicationOptions.class);
	
	boolean connectionFromSlave = false;
	ListSet<TableCacheKey> filteredTables = new ListSet<TableCacheKey>();
	
	
	public ReplicationOptions() {
	}
	
	public void addTableFilter(SchemaContext sc, String database, String table) {
		if (StringUtils.isEmpty(database) || StringUtils.isEmpty(table)) {
			logger.warn("Invalid filter value specified.  DB='" + ((database == null) ? "null" : database) + 
					"', TABLE='" + ((table == null) ? "null" : table) + "'");
			return;
		}
		
		SchemaCacheKey<Database<?>> sckdb = PEDatabase.getDatabaseKey(new UnqualifiedName(database));
		if (sckdb == null) {
			logger.warn("Invalid database filter value specified.  DB='" + database + "'");
			return;
		}

		TableCacheKey tck = null;
		Database<?> db = sckdb.load(sc);
		if (db == null) {
			tck = new TableCacheKey(0, database, new UnqualifiedName(table));
		} else {
			if (db instanceof PEDatabase) {
				PEDatabase pedb = (PEDatabase) db;
	
				PEAbstractTable<?> petbl = sc.findTable(PEAbstractTable.getTableKey(pedb, new UnqualifiedName(table)));
				if (petbl == null) {
					tck = new TableCacheKey(pedb.getId(), pedb.getName().get(), new UnqualifiedName(table));
				} else {
					tck = (TableCacheKey)(petbl.getCacheKey());
				}
			}
		}
		if (tck != null) {
			filteredTables.add(tck);
		}
	}

	public ListSet<TableCacheKey> getFilteredTables() {
		return filteredTables;
	}

	public boolean connectionFromReplicationSlave() {
		return connectionFromSlave;
	}
	
	public void setConnectionFromReplicationSlave(boolean val) {
		this.connectionFromSlave = val;
	}
}

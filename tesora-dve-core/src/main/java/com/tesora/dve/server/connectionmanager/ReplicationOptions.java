// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

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

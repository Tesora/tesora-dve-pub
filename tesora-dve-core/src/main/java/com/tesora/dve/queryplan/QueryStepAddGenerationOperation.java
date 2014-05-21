// OS_STATUS: public
package com.tesora.dve.queryplan;

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

import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageGroupGeneration;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepAddGenerationOperation extends QueryStepOperation {
	
	PersistentGroup group;
	List<PersistentSite> sites;
	CacheInvalidationRecord record;
	
	// this is the list of tables to declare and copy (bcast only)
	// some of these may not be actual tables - they may be views (check via UserTable.getView)
	// this is a valid declaration order
	ListOfPairs<UserTable,SQLCommand> tableDecls;
	// if true we have to disable fks in order to copy data.  only set if there is a cycle amongst 
	// bcast tables.
	boolean mustIgnoreFKs;
	// all the users and grants
	List<SQLCommand> userDecls;
	
	public QueryStepAddGenerationOperation(PersistentGroup sg, List<PersistentSite> sites, CacheInvalidationRecord invalidate) {
		this(sg,sites,invalidate,null,false,null);
	}
	
	public QueryStepAddGenerationOperation(PersistentGroup sg, List<PersistentSite> sites, CacheInvalidationRecord invalidate,
			ListOfPairs<UserTable,SQLCommand> tableDecls,
			boolean ignoreFKs,
			List<SQLCommand> userDecls) {
		this.group = sg;
		this.sites = sites;
		this.record = invalidate;
		this.tableDecls = tableDecls;
		this.mustIgnoreFKs = ignoreFKs;
		this.userDecls = userDecls;
	}

	@Override
	public void execute(final SSConnection ssCon, final WorkerGroup wg,	DBResultConsumer resultConsumer) throws Throwable {
		try {
			if (record != null)
				QueryPlanner.invalidateCache(record);
			ssCon.getCatalogDAO().new EntityGenerator() {
				@Override
				public CatalogEntity generate() throws Throwable {
					StorageGroupGeneration newGen = new StorageGroupGeneration(group, group.getGenerations().size(), sites);
					ssCon.getCatalogDAO().persistToCatalog(newGen);
					group.addGeneration(ssCon, wg, newGen);
					return newGen;
				}
			}.execute();
		} finally {
			if (record != null)
				QueryPlanner.invalidateCache(record);
		}
	}

	@Override
	public boolean requiresTransaction() {
		return false;
	}

	@Override
	public boolean requiresImplicitCommit() {
		return true;
	}

}

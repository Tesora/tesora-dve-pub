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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.*;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.statement.ddl.AddStorageGenRangeInfo;
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
	// if we're rebalancing this is nonempty
	List<AddStorageGenRangeInfo> rebalanceInfo;
	
	public QueryStepAddGenerationOperation(PersistentGroup sg, List<PersistentSite> sites, CacheInvalidationRecord invalidate) {
		this(sg,sites,invalidate,null,false,null, Collections.<AddStorageGenRangeInfo> emptyList());
	}
	
	public QueryStepAddGenerationOperation(PersistentGroup sg, List<PersistentSite> sites, CacheInvalidationRecord invalidate,
			ListOfPairs<UserTable,SQLCommand> tableDecls,
			boolean ignoreFKs,
			List<SQLCommand> userDecls,
			List<AddStorageGenRangeInfo> rebalanceInfo) {
		this.group = sg;
		this.sites = sites;
		this.record = invalidate;
		this.tableDecls = tableDecls;
		this.mustIgnoreFKs = ignoreFKs;
		this.userDecls = userDecls;
		this.rebalanceInfo = rebalanceInfo;
	}

	@Override
	public void execute(final SSConnection ssCon, final WorkerGroup wg,	DBResultConsumer resultConsumer) throws Throwable {
		try {
			if (record != null)
				QueryPlanner.invalidateCache(record);
            final CatalogDAO catalogDAO = ssCon.getCatalogDAO();
            catalogDAO.new EntityGenerator() {
				@Override
				public CatalogEntity generate() throws Throwable {
                    //NOTE: this catalog entry is for the latest storage generation.
                    StorageGroupGeneration newGen = new StorageGroupGeneration(group, group.getGenerations().size(), sites);
                    if (rebalanceInfo == null)
					    catalogDAO.persistToCatalog(newGen);
                    //NOTE: this call is responsible for scanning the dist-key ranges and setting up any catalog entries for older generations.
                    group.addGeneration(ssCon, wg, newGen, tableDecls, mustIgnoreFKs, userDecls, rebalanceInfo);
					return rebalanceInfo == null ? newGen : null;
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

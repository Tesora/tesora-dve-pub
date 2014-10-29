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

import com.tesora.dve.common.catalog.*;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.GenerationKeyRange;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.statement.ddl.AddStorageGenRangeInfo;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.worker.WorkerGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 */
public class QueryStepRebalance extends QueryStepOperation {
    static final Logger log = LoggerFactory.getLogger(QueryStepRebalance.class);

    PersistentGroup group;
    List<AddStorageGenRangeInfo> rebalanceInfo;
    boolean ignoreFKs;
    
    public QueryStepRebalance(PersistentGroup group, List<AddStorageGenRangeInfo> rebalanceInfo, boolean ignoreFKs) {
        this.group = group;
        this.rebalanceInfo = rebalanceInfo;
        this.ignoreFKs = ignoreFKs;
    }

    @Override
    public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		boolean fkVal = KnownVariables.FOREIGN_KEY_CHECKS.getSessionValue(ssCon);
    	try {
    		if (ignoreFKs) 
    			ssCon.setSessionVariable("foreign_key_checks", "0");

    		this.migrateAllGenerationsToCurrent(ssCon, wg);
    	} finally {
    		if (ignoreFKs)
    			ssCon.setSessionVariable("foreign_key_checks", (fkVal ? "1" : "0"));
    	}
    }

    private void migrateAllGenerationsToCurrent(SSConnection ssCon, WorkerGroup wg) throws PEException {
        List<StorageGroupGeneration> generations = group.getGenerations();
        //NOTE: Assumes previous storage gen add has copied broadcast tables to all p-sites, while random data and ranged data have not been copied.

        log.debug("migrating old generations on group {}",group.getName());

        Set<PersistentSite> sitesInOldGens = new LinkedHashSet<>();

        ArrayList<StorageGroupGeneration> oldGenerations = new ArrayList<>();

        for (int i=(generations.size() - 2);i>=0;i--) { //collapse youngest to oldest to avoid multiple moves.
            StorageGroupGeneration oldGen = generations.get(i);
            oldGenerations.add(oldGen);
            for (PersistentSite site : oldGen.getStorageSites())
                sitesInOldGens.add(site);
        }

        if (oldGenerations.isEmpty()) {
            log.debug("Found no old generations to compact, exiting.");
            return;
        } else {
            log.debug("Found {} old generations to compact.",oldGenerations.size());
        }

        StorageGroupGeneration newGen = group.getLastGen();

        PersistentGroup group = newGen.getStorageGroup();

        //TODO: We will eventually need to deal with down-sizing and balancing, even for random data.
        //TODO: Broadcast data has already been copied for up sizing, but we will eventually need to deal with down-sizing.

        for (StorageGroupGeneration oldGen : oldGenerations) {
            //NOTE: this visits all old generations except the current gen in youngest to oldest order, which will properly migrate overlapping storage gens into the current gen.
            log.debug("Processing old generation, version={}", oldGen.getVersion() );

            for (AddStorageGenRangeInfo rebalanceEntry : rebalanceInfo){
                //TODO: we need to start an XA here.  Eventually we can split generation/key range, which would let us update a key range after moving some data for just one site.
                RangeDistribution schemaRange = rebalanceEntry.getRange();
                DistributionRange range = schemaRange.getPersistent(ssCon.getSchemaContext());
                log.debug("Processing range={}", range.getName());

                for (PersistentSite oldSite : oldGen.getStorageSites()) {
                    //TODO: right now, we move all rows.  In the future we can move fewer rows, as long as we move all rows for dist-key / FK.
                    QueryStepShardMovement shardMove = new QueryStepShardMovement(oldGen, range, oldSite, newGen, rebalanceEntry);
                    shardMove.execute(ssCon,wg, DBEmptyTextResultConsumer.INSTANCE);
                }

                //TODO:update catalog entries, commit XA, invalidate any key range related caching.
                GenerationKeyRange keyBoundary  = range.removeRangeForGeneration(oldGen);
                ssCon.getCatalogDAO().persistToCatalog(range);
                ssCon.getCatalogDAO().remove(keyBoundary);
            }
            //OK, we've processed everything in this old generation.
            group.removeGeneration(oldGen);
            ssCon.getCatalogDAO().persistToCatalog(group);
            ssCon.getCatalogDAO().remove(oldGen);

        }
    }


}

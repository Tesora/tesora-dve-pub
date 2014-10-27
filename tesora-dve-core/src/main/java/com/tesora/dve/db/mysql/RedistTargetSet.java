package com.tesora.dve.db.mysql;

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

import com.tesora.dve.common.PECollectionUtils;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.CommandChannel;
import com.tesora.dve.db.mysql.libmy.MyBinaryResultRow;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.db.mysql.portal.protocol.CanFlowControl;
import com.tesora.dve.db.mysql.portal.protocol.DownstreamFlowControlSet;
import com.tesora.dve.db.mysql.portal.protocol.FlowControl;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerGroup;

import java.util.*;

/**
 *
 */
public class RedistTargetSet implements RedistTargetSite.InsertWatcher, RedistTargetSite.InsertPolicy, CanFlowControl {

    final WorkerGroup targetWG;
    final RedistTargetSite.InsertWatcher watcher;
    RedistTargetSite.InsertPolicy policy;

    final Map<StorageSite, RedistTargetSite> siteCtxBySite = new HashMap<>();
    final DownstreamFlowControlSet siteFlowControl = new DownstreamFlowControlSet();

    boolean alreadyFailed = false;
    boolean alreadyClosed = false;

    long insertedRows = 0;

    public RedistTargetSet(WorkerGroup targetWG, RedistTargetSite.InsertWatcher watcher, RedistTargetSite.InsertPolicy policy, FlowControl sourceControl) throws PEException {
        this.targetWG = targetWG;
        this.watcher = watcher;
        this.policy = policy;
        siteFlowControl.setUpstreamControl(sourceControl);

        Collection<Worker> targetWorkers = targetWG.getTargetWorkers(WorkerGroup.MappingSolution.AllWorkers);
        for (Worker targetWorker : targetWorkers){
            CommandChannel directChannel = targetWorker.getDirectChannel();
            StorageSite site = directChannel.getStorageSite();

            RedistTargetSite siteCtx = new RedistTargetSite(this, directChannel, this);
            siteFlowControl.register(siteCtx);
            siteCtxBySite.put(site, siteCtx);
        }
    }


    public boolean sendInsert(WorkerGroup.MappingSolution mappingSolution, MyBinaryResultRow binRow) throws PEException {
        if (alreadyFailed || alreadyClosed)
            return false;

        Collection<RedistTargetSite> allTargetSites = chooseTargetSites(mappingSolution);
        boolean flushedOne = false;
        for (RedistTargetSite siteCtx : allTargetSites) {
            flushedOne = flushedOne || siteCtx.append(binRow);
        }

        return flushedOne;
    }

    private Collection<RedistTargetSite> chooseTargetSites(WorkerGroup.MappingSolution mappingSolution) throws PEException {
        Collection<RedistTargetSite> allTargetSites;
        if (mappingSolution == WorkerGroup.MappingSolution.AllWorkers || mappingSolution == WorkerGroup.MappingSolution.AllWorkersSerialized) {
            //this is broadcast, we send to all workers.
            allTargetSites = siteCtxBySite.values();
        } else if (mappingSolution == WorkerGroup.MappingSolution.AnyWorker || mappingSolution == WorkerGroup.MappingSolution.AnyWorkerSerialized) {
            //this is random, we send to any of the workers.
            allTargetSites = Collections.singleton(PECollectionUtils.selectRandom(siteCtxBySite.values()));
        } else {
            //this is range, we send to a specific worker based on the previously computed distribution vector of the row
            allTargetSites = Collections.singleton(siteCtxBySite.get(targetWG.resolveSite(mappingSolution.getSite())));
        }
        return allTargetSites;
    }

    public void close() {
        for (RedistTargetSite siteCtx : siteCtxBySite.values()) {
            siteCtx.close();
            siteFlowControl.unregister(siteCtx);
        }
        alreadyClosed = true;
    }

    public void flush() {
        for (RedistTargetSite siteContext : siteCtxBySite.values()) {
            siteContext.flush();
        }
    }

    public boolean hasPendingRows() {
        boolean anySiteHasPending = false;
        for (RedistTargetSite siteCtx : siteCtxBySite.values()) {
            if (siteCtx.hasPendingRows()){
                anySiteHasPending = true;
                break;
            }
        }
        return anySiteHasPending;
    }

    @Override
    public int getMaximumRowsToBuffer() {
        return policy.getMaximumRowsToBuffer();
    }

    @Override
    public long getMaximumBytesToBuffer() {
        return policy.getMaximumBytesToBuffer();
    }

    @Override
    public int getColumnsPerTuple() {
        return policy.getColumnsPerTuple();
    }

    @Override
    public SQLCommand buildInsertStatement(int tupleCount) throws PEException {
        return policy.buildInsertStatement(tupleCount);
    }

    @Override
    public void insertOK(RedistTargetSite siteCtx, MyOKResponse okPacket) {
        if (alreadyFailed || alreadyClosed)
            return;

        long affectedRows = okPacket.getAffectedRows();
        try {
            int rowCount = (int) affectedRows;
            insertedRows += rowCount;
        } finally {
            watcher.insertOK(siteCtx,okPacket);
        }
    }

    @Override
    public void insertFailed(RedistTargetSite site, MyErrorResponse errorPacket) {
        this.alreadyFailed = true;
        watcher.insertFailed(site,errorPacket);
    }

    @Override
    public void insertFailed(RedistTargetSite site, Exception e) {
        this.alreadyFailed = true;
        watcher.insertFailed(site,e);
    }

    public long getUpdatedRowCount() {
        return insertedRows;
    }

    @Override
    public void setUpstreamControl(FlowControl control) {
        if (control == this || control == siteFlowControl)
            throw new IllegalArgumentException();

        siteFlowControl.setUpstreamControl(control);
    }

}

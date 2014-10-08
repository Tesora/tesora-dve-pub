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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.concurrent.*;
import com.tesora.dve.db.CommandChannel;
import com.tesora.dve.db.mysql.portal.protocol.StreamValve;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.queryplan.QueryStepMultiTupleRedistOperation;
import com.tesora.dve.queryplan.TableHints;
import com.tesora.dve.worker.MysqlRedistTupleForwarder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.common.PECollectionUtils;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class RedistTupleBuilder implements RedistTargetSite.InsertPolicy {
    static final Logger logger = Logger.getLogger(RedistTupleBuilder.class);
    static final String SIMPLE_CLASSNAME = RedistTupleBuilder.class.getSimpleName();

	static private AtomicInteger nextId = new AtomicInteger();
	private int thisId = nextId.incrementAndGet();



    Map<StorageSite, RedistTargetSite> siteCtxBySite = new HashMap<>();

    IdentityHashMap<RedistTargetSite,RedistTargetSite> blockedTargetSites = new IdentityHashMap<>();
    IdentityHashMap<ChannelHandlerContext,ChannelHandlerContext> sourceSites = new IdentityHashMap<>();

	int updatedRowsCount = 0;

	final PEDefaultPromise<Integer> completionPromise = new PEDefaultPromise<>();

    boolean sourcePaused = false;

	boolean lastPacketSent = false;
    boolean failedRedist = false;

	final Future<SQLCommand> insertStatementFuture;
	final PersistentTable targetTable;
	final WorkerGroup targetWG;
    final CatalogDAO catalogDAO;
    final DistributionModel distModel;

	final int maximumRowCount;
	final int maxDataSize;
	final SQLCommand insertOptions;
	boolean insertIgnore = false;

    TableHints tableHints;
    MysqlRedistTupleForwarder.MaximumAutoIncr maxAutoIncr = null;
	private ColumnSet rowSetMetadata;

	public RedistTupleBuilder(CatalogDAO catalogDAO, DistributionModel distModel, Future<SQLCommand> insertStatementFuture, SQLCommand insertOptions,
                              PersistentTable targetTable, int maximumRowCount, int maxDataSize,
                              WorkerGroup targetWG) {
        this.distModel = distModel;
        this.catalogDAO = catalogDAO;
		this.insertOptions = insertOptions;
		this.insertStatementFuture = insertStatementFuture;
		this.targetTable = targetTable;
		this.targetWG = targetWG;
		this.maximumRowCount = maximumRowCount;
		this.maxDataSize = maxDataSize;
	}


    public void processSourceRow(KeyValue distValue, List<MysqlRedistTupleForwarder.ColumnValueInspector> columnInspectorList, MyBinaryResultRow binRow) throws PEException {
        //this get's called by MysqlRedistTupleForwarder when we receive a new row from a source query.
        try {
            innerHandleRow(distValue, columnInspectorList, binRow);
        } catch (Exception e){
            this.failure(e);
        }
    }

    private void innerHandleRow(KeyValue distValue, List<MysqlRedistTupleForwarder.ColumnValueInspector> columnInspectorList, MyBinaryResultRow binRow) throws PEException {
        if (failedRedist)
            return;//drop our source rows quickly if redist has already failed.

        long[] autoIncrBlocks = null;

        if (tableHints.tableHasAutoIncs()) {
            //TODO: this call fetches a single autoinc, which might block on hibernate if the background pre-fetch is slow, stalling the netty thread.  sgossard
            autoIncrBlocks = tableHints.buildBlocks(catalogDAO, 1 /*rowcount*/); //gets an autoinc for one row.
        }

        if (tableHints.usesExistingAutoIncs() && maxAutoIncr == null)
            maxAutoIncr = new MysqlRedistTupleForwarder.MaximumAutoIncr();

        MappingSolution mappingSolution;
        if ( BroadcastDistributionModel.SINGLETON.equals(distModel) && !tableHints.isUsingAutoIncColumn()) {
            mappingSolution = MappingSolution.AllWorkersSerialized;
        } else {
            long nextAutoIncr = tableHints.tableHasAutoIncs() ? autoIncrBlocks[0] : 0;

            KeyValue dv = new KeyValue(distValue);

            //picks apart the row, looking at the distribution keys and autoinc fields.
            for (int i = 0; i < columnInspectorList.size(); ++i) {
                MysqlRedistTupleForwarder.ColumnValueInspector dvm = columnInspectorList.get(i);
                dvm.inspectValue(binRow, i, dv, maxAutoIncr);
            }

            mappingSolution = distModel.mapKeyForInsert(catalogDAO, targetWG.getGroup(), dv);

            autoIncrBlocks = tableHints.tableHasAutoIncs() ? new long[] {nextAutoIncr++} : null;
        }

        boolean flushedInserts = false;
        //********************
        long[] autoIncrBlocks1 = autoIncrBlocks;
        Collection<RedistTargetSite> allTargetSites = chooseTargetSites(mappingSolution);

        boolean shouldCopyAutoIncs = (allTargetSites.size() > 1);
        for (RedistTargetSite siteCtx : allTargetSites){
            if (shouldCopyAutoIncs)
                autoIncrBlocks1 = (autoIncrBlocks1 == null) ? null : new long[]{autoIncrBlocks1[0]};
            try {

                int rowsToFlushCount = 1;
                int bytesToFlushCount = binRow.sizeInBytes();


                boolean needsFlush = (siteCtx.getTotalQueuedRows() + rowsToFlushCount >= maximumRowCount) || siteCtx.getTotalQueuedBytes() + bytesToFlushCount >= maxDataSize;

                long[] autoIncUsed = autoIncrBlocks1;
                if (needsFlush && autoIncrBlocks1 != null) {
                    autoIncUsed = new long[] { autoIncrBlocks1[0] };
                    autoIncrBlocks1[0] += rowsToFlushCount;
                }

                siteCtx.append(binRow, rowsToFlushCount, bytesToFlushCount, autoIncUsed);

                if (siteCtx.getTotalQueuedRows() >= maximumRowCount || siteCtx.getTotalQueuedBytes() >= maxDataSize){
                    flushedInserts = true;
                    siteCtx.flush();
                    if (! siteCtx.willAcceptMoreRows() ){
                        blockedTargetSites.put(siteCtx,siteCtx);
                    }
                }

            } finally {
    //			siteCtx.siteCtxLock.unlock();
            }
        }
        //********************

        if (!blockedTargetSites.isEmpty()){
            pauseSourceStreams();
        }

        //TODO: this should really be called BEFORE flushes, to ensure we don't lose a tracked autoinc on a failure. -sgossard
        if (flushedInserts)
            updateAutoIncIfNeeded();
    }

    private void updateAutoIncIfNeeded() {
        if (maxAutoIncr != null && maxAutoIncr.isSet() && tableHints.isUsingAutoIncColumn()){
            //TODO: this call saves the maximum autoinc via hibernate, and will stall the netty thread.  sgossard
            tableHints.recordMaximalAutoInc(catalogDAO, maxAutoIncr.getMaxValue());
            maxAutoIncr = null;
        }
    }

    private Collection<RedistTargetSite> chooseTargetSites(MappingSolution mappingSolution) throws PEException {
        Collection<RedistTargetSite> allTargetSites;
        if (mappingSolution == MappingSolution.AllWorkers || mappingSolution == MappingSolution.AllWorkersSerialized) {
            //this is broadcast, we send to all workers.
            allTargetSites = siteCtxBySite.values();
        } else if (mappingSolution == MappingSolution.AnyWorker || mappingSolution == MappingSolution.AnyWorkerSerialized) {
            //this is random, we send to any of the workers.
            allTargetSites = Collections.singleton(PECollectionUtils.selectRandom(siteCtxBySite.values()));
        } else {
            //this is range, we send to a specific worker based on the previously computed distribution vector of the row
            allTargetSites = Collections.singleton(siteCtxBySite.get(targetWG.resolveSite(mappingSolution.getSite())));
        }
        return allTargetSites;
    }


    public void setProcessingComplete() throws PEException {
        //Called when upstream forwarder has seen stream EOFs from all source streams, so all rows have been forwarded.

        //unpause all source streams so they are ready for re-use.
        resumeSourceStreams();

        lastPacketSent = true;
        flushTargetSites();

        testRedistributionComplete();
    }

    public void failure(Exception e) {
        failedRedist = true;
        completionPromise.failure(e);
    }

    public void addSite(CommandChannel channel) {
        StorageSite site = channel.getStorageSite();
        RedistTargetSite siteCtx = new RedistTargetSite(this, channel, this);
        siteCtxBySite.put(site, siteCtx);
    }

    public int getUpdateCount() throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("redist # "+thisId+" , about to call completionPromise.sync(): " + completionPromise);
        return completionPromise.sync();
    }

    @Override
    public int getMaximumRowsToBuffer() {
        return maximumRowCount;
    }

    @Override
    public int getColumnsPerTuple() {
        return targetTable.getNumberOfColumns();
    }

    @Override
    public ColumnSet getRowsetMetadata() {
        return rowSetMetadata;
    }

	public SQLCommand buildInsertStatement(int tupleCount) throws PEException {
        SQLCommand insertCommand;
        if (tupleCount == maximumRowCount && insertStatementFuture != null) {
            try {
                //TODO: this delayed building is suspect, we don't want to block a netty thread, and it ignores the tuple count. -sgossard
                insertCommand = insertStatementFuture.get();
            } catch (ExecutionException ee) {
                throw new PEException("Exception encountered syncing to redist insert statement", ee);
            } catch (InterruptedException ie) {
                throw new PEException("Sync to redist insert statement interrupted", ie);
            }
        } else {
			insertCommand = QueryStepMultiTupleRedistOperation.getTableInsertStatement(
					/*
					 * PerHostConnectionManager.INSTANCE.lookupConnection(targetWG
					 * .getCommectionId())
					 */CharsetUtil.UTF_8, targetTable, insertOptions, rowSetMetadata, tupleCount,
					insertIgnore);
        }
        return insertCommand;
    }


    public void setRowSetMetadata(TableHints tableHints, ColumnSet resultColumnMetadata) {
        this.tableHints = tableHints;
        this.rowSetMetadata = resultColumnMetadata;
    }

    public void setInsertIgnore(boolean insertIgnore) {
        this.insertIgnore = insertIgnore;
    }

    @Override
    public String toString() {
        return SIMPLE_CLASSNAME + "{" + thisId + "}";
    }


    public void sourceActive(ChannelHandlerContext ctx){
        //this is called by MysqlRedistTupleForwarder instances that are processing the source queries when they are in position to receive response packets.
        if (sourceSites.get(ctx) == null){
            sourceSites.put(ctx,ctx);

            //new site, pause it if we are paused.
            if (sourcePaused) {
                StreamValve.pipelinePause(ctx.pipeline());
            }
        }
    }

    protected void pauseSourceStreams(){
        sourcePaused = true;
        for (ChannelHandlerContext ctx : sourceSites.keySet()){
            StreamValve.pipelinePause(ctx.pipeline());
        }
    }

    protected void resumeSourceStreams(){
        if (sourcePaused){
            sourcePaused = false;
            for (ChannelHandlerContext ctx : sourceSites.keySet()){
                StreamValve.pipelineResume(ctx.pipeline());
            }
        }
    }


    public boolean processTargetPacket(RedistTargetSite siteCtx, MyMessage message) {

        if (!isProcessingComplete(siteCtx)) {
            try {
                if (message instanceof MyOKResponse) {
                    if (!isProcessingComplete(siteCtx) && !completionPromise.isFulfilled()) { // skip if previous exception
                        int rowCount = (int) ((MyOKResponse)message).getAffectedRows();
                        updatedRowsCount+= rowCount;
                    }
                } else {
                    MyErrorResponse err = (MyErrorResponse)message;
                    failure(err.asException());
                }
            } finally {
                siteCtx.handleAck(message);
                checkIfSitesAreUnblocked();
            }
        }

        testRedistributionComplete();

        return isProcessingComplete(siteCtx);
    }

    private void checkIfSitesAreUnblocked(){
        Iterator<RedistTargetSite> blockedSites = blockedTargetSites.keySet().iterator();
        while (blockedSites.hasNext()){
            RedistTargetSite site = blockedSites.next();
            if (site.willAcceptMoreRows())
                blockedSites.remove();
        }
        if (blockedTargetSites.isEmpty()) {
            resumeSourceStreams();
        }
    }

    private boolean isProcessingComplete(RedistTargetSite siteCtx) {
        boolean done = lastPacketSent && !siteCtx.hasPendingRows();
        return done;
	}
	


    private void closeTargetSites() {

        for (RedistTargetSite siteCtx : siteCtxBySite.values()) {
            siteCtx.close();
        }
    }

    private void flushTargetSites() {

        updateAutoIncIfNeeded();

        for (RedistTargetSite siteContext : siteCtxBySite.values()) {
            siteContext.flush();
        }
    }

    private void testRedistributionComplete() {
        boolean isProcessingComplete = lastPacketSent;
		for (RedistTargetSite siteCtx : siteCtxBySite.values()) {
			if ( siteCtx.hasPendingRows() ) {
				isProcessingComplete = false;
				break;
			}
		}

		if (isProcessingComplete) {
			if (logger.isDebugEnabled())
				logger.debug("redist # "+thisId+" , redistribution of " + targetTable.displayName() + " complete - " + updatedRowsCount + " rows updated");
            try{
                closeTargetSites();
                completionPromise.trySuccess(updatedRowsCount);
            } catch (Exception e){
                completionPromise.failure(e);
            }

		}
	}


}